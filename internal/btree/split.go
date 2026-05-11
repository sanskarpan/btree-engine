package btree

import (
	"bytes"

	"btree-engine/internal/events"
	"btree-engine/internal/mvcc"
	"btree-engine/internal/page"
)

// splitLeaf splits the given leaf page, inserts the new tuple, and propagates the
// separator key upward. leafPageID must already be unpinned before this call.
func (bt *BPlusTree) splitLeaf(txn *mvcc.Transaction, leafPageID uint32, parents []uint32,
	newKey []byte, newTuple *mvcc.MVCCTuple) error {

	// Re-fetch the leaf (it was unpinned by Insert before calling us)
	leafFrame, err := bt.bufPool.FetchPage(leafPageID)
	if err != nil {
		return err
	}
	leaf := bt.bufPool.ReadPageData(leafFrame)
	leafHdr := leaf.DecodeHeader()
	oldFlags := leafHdr.Flags
	oldNextSib := leafHdr.NextSib

	// 1. Allocate new right page
	rightID, err := bt.disk.AllocPage()
	if err != nil {
		bt.bufPool.UnpinPage(leafPageID, false)
		return err
	}
	rightFrame, err := bt.bufPool.FetchPage(rightID)
	if err != nil {
		bt.bufPool.UnpinPage(leafPageID, false)
		return err
	}
	right := bt.bufPool.ReadPageData(rightFrame)
	right.ID = rightID
	right.EncodeHeader(page.PageHeader{
		Type:      page.PageTypeLeaf,
		Flags:     oldFlags &^ page.FlagIsRoot, // new page is not the root
		FreeStart: page.HeaderSize,
		FreeEnd:   page.PageSize,
		PrevSib:   leafPageID,
		NextSib:   oldNextSib,
	})

	// 2. WAL: log the split BEFORE modifying pages
	var lsn uint64
	if bt.walMgr != nil {
		lsn = bt.walMgr.LogSplitRecord(uint64(txn.ID), uint64(leafPageID), uint64(rightID), txn.LastLSN, newKey, newTuple.Encode())
		txn.LastLSN = lsn
		leaf.SetPageLSN(lsn)
		right.SetPageLSN(lsn)
	}

	// 3. Move top half of leaf's slots to right page
	numSlots := leaf.NumSlots()
	midSlot := numSlots / 2
	for i := midSlot; i < numSlots; i++ {
		data := leaf.GetRecord(i)
		if data == nil {
			continue
		}
		right.InsertRecord(data, mvcc.TupleKeyExtract) //nolint
		leaf.DeleteRecord(i)
	}
	leaf.Compact()

	// 4. Update sibling links: leaf -> right -> oldNext
	leaf.SetNextSib(rightID)
	right.SetPrevSib(leafPageID)
	right.SetNextSib(oldNextSib)
	if oldNextSib != 0 {
		nextFrame, err := bt.bufPool.FetchPage(oldNextSib)
		if err == nil {
			next := bt.bufPool.ReadPageData(nextFrame)
			next.SetPrevSib(rightID)
			bt.bufPool.WritePageData(oldNextSib, next)
			bt.bufPool.UnpinPage(oldNextSib, true)
		}
	}

	// 5. Insert the new tuple into the correct half
	rightLP := &LeafPage{right}
	rightMinKey := mvcc.TupleKeyExtract(right.GetRecord(0))
	if bytes.Compare(newKey, rightMinKey) >= 0 {
		rightLP.InsertTuple(newTuple) //nolint
	} else {
		leftLP := &LeafPage{leaf}
		leftLP.InsertTuple(newTuple) //nolint
	}

	// 6. Write back and mark both dirty
	bt.bufPool.WritePageData(leafPageID, leaf)
	bt.bufPool.WritePageData(rightID, right)
	bt.bufPool.UnpinPage(leafPageID, true)
	bt.bufPool.UnpinPage(rightID, true)

	// Rebuild bloom filters: left page lost some keys, right page is new.
	bt.rebuildPageBloom(leafPageID, leaf)
	bt.rebuildPageBloom(rightID, right)

	// 7. Promote separator key to parent
	separatorKey := mvcc.TupleKeyExtract(right.GetRecord(0))
	if bt.bus != nil {
		bt.bus.Publish(events.Event{Type: events.EvtTreeSplit,
			Extra: map[string]interface{}{
				"left_page": leafPageID, "right_page": rightID,
				"separator": string(separatorKey),
			}})
	}
	return bt.insertIntoParent(txn, parents, leafPageID, separatorKey, rightID)
}

// insertIntoParent inserts a separator key and right-child pointer into the parent.
// If there is no parent (the leaf WAS the root), a new root is created.
func (bt *BPlusTree) insertIntoParent(txn *mvcc.Transaction, parents []uint32,
	leftID uint32, separatorKey []byte, rightID uint32) error {

	if len(parents) == 0 {
		// The split page was the root; create a new root
		return bt.createNewRoot(txn, leftID, separatorKey, rightID)
	}

	parentID := parents[len(parents)-1]
	parentFrame, err := bt.bufPool.FetchPage(parentID)
	if err != nil {
		return err
	}
	parentPage := bt.bufPool.ReadPageData(parentFrame)
	ip := &InternalPage{parentPage}

	// WAL: log the separator insertion into the parent before modifying it
	if bt.walMgr != nil {
		sepEncoded := EncodeInternalRecord(InternalRecord{Key: separatorKey, ChildID: rightID})
		sepLSN := bt.walMgr.LogInsertRecord(uint64(txn.ID), uint64(parentID), txn.LastLSN, sepEncoded)
		txn.LastLSN = sepLSN
		parentPage.SetPageLSN(sepLSN)
	}

	err = ip.InsertKey(separatorKey, rightID)
	if err != nil {
		// Parent is full — split the internal node
		bt.bufPool.WritePageData(parentID, parentPage)
		bt.bufPool.UnpinPage(parentID, true)
		return bt.splitInternal(txn, parentID, parents[:len(parents)-1], separatorKey, rightID)
	}

	bt.bufPool.WritePageData(parentID, parentPage)
	bt.bufPool.UnpinPage(parentID, true)
	return nil
}

// splitInternal splits an internal page when it overflows.
func (bt *BPlusTree) splitInternal(txn *mvcc.Transaction, internalPageID uint32,
	parents []uint32, newKey []byte, newChildID uint32) error {

	internalFrame, err := bt.bufPool.FetchPage(internalPageID)
	if err != nil {
		return err
	}
	internal := bt.bufPool.ReadPageData(internalFrame)

	rightID, err := bt.disk.AllocPage()
	if err != nil {
		bt.bufPool.UnpinPage(internalPageID, false)
		return err
	}
	rightFrame, err := bt.bufPool.FetchPage(rightID)
	if err != nil {
		bt.bufPool.UnpinPage(internalPageID, false)
		return err
	}
	right := bt.bufPool.ReadPageData(rightFrame)
	right.ID = rightID
	right.Init(page.PageTypeInternal)

	numSlots := internal.NumSlots()
	midSlot := numSlots / 2

	// Collect mid key to push up
	midData := internal.GetRecord(midSlot)
	midRec, _ := DecodeInternalRecord(midData)
	pushUpKey := midRec.Key
	rightIP := &InternalPage{right}
	rightIP.SetLeftmostChild(midRec.ChildID)

	// Move top half (after mid) to right
	for i := midSlot + 1; i < numSlots; i++ {
		data := internal.GetRecord(i)
		if data == nil {
			continue
		}
		right.InsertRecord(data, InternalKeyExtract) //nolint
		internal.DeleteRecord(i)
	}
	internal.DeleteRecord(midSlot)
	internal.Compact()

	// Insert new key into correct half
	newRec := InternalRecord{Key: newKey, ChildID: newChildID}
	encoded := EncodeInternalRecord(newRec)
	if bytes.Compare(newKey, pushUpKey) >= 0 {
		right.InsertRecord(encoded, InternalKeyExtract) //nolint
	} else {
		internal.InsertRecord(encoded, InternalKeyExtract) //nolint
	}

	bt.bufPool.WritePageData(internalPageID, internal)
	bt.bufPool.WritePageData(rightID, right)
	bt.bufPool.UnpinPage(internalPageID, true)
	bt.bufPool.UnpinPage(rightID, true)

	return bt.insertIntoParent(txn, parents, internalPageID, pushUpKey, rightID)
}

// createNewRoot creates a new root internal page with leftID and rightID as children.
func (bt *BPlusTree) createNewRoot(txn *mvcc.Transaction, leftID uint32, separatorKey []byte, rightID uint32) error {
	newRootID, err := bt.disk.AllocPage()
	if err != nil {
		return err
	}
	newRootFrame, err := bt.bufPool.FetchPage(newRootID)
	if err != nil {
		return err
	}
	root := bt.bufPool.ReadPageData(newRootFrame)
	root.ID = newRootID
	root.Init(page.PageTypeInternal)
	root.SetFlag(page.FlagIsRoot)

	rootIP := &InternalPage{root}
	rootIP.SetLeftmostChild(leftID)
	rootIP.SetRightmostChild(rightID)
	rootIP.InsertKey(separatorKey, rightID) //nolint

	// WAL: log new root creation with full structure BEFORE writing the page
	if bt.walMgr != nil {
		rootLSN := bt.walMgr.LogRootChangeRecord(newRootID, leftID, rightID, separatorKey, txn.LastLSN)
		txn.LastLSN = rootLSN
		root.SetPageLSN(rootLSN)
	}

	bt.bufPool.WritePageData(newRootID, root)
	bt.bufPool.UnpinPage(newRootID, true)

	// Clear old root's IsRoot flag
	oldRootFrame, err := bt.bufPool.FetchPage(leftID)
	if err == nil {
		oldRoot := bt.bufPool.ReadPageData(oldRootFrame)
		oldRoot.ClearFlag(page.FlagIsRoot)
		bt.bufPool.WritePageData(leftID, oldRoot)
		bt.bufPool.UnpinPage(leftID, true)
	}

	bt.rootPageID = newRootID
	if bt.onRootChange != nil {
		bt.onRootChange(newRootID)
	}
	return nil
}
