package btree

import (
	"bytes"
	"encoding/binary"

	"btree-engine/internal/buffer"
	"btree-engine/internal/events"
	"btree-engine/internal/mvcc"
	"btree-engine/internal/page"
	"btree-engine/internal/wal"
)

// minLeafFill is the minimum number of physical (non-tombstone) slots before a leaf is considered
// underflowing. This only matters after VACUUM has compacted the page.
const minLeafFill = 3

// handleUnderflow is called after a leaf modification. If the leaf has too few physical records
// it tries borrow-from-right, then borrow-from-left, then merge with a sibling.
// It unpins the leaf and ALL parent frames before returning.
func (bt *BPlusTree) handleUnderflow(txn *mvcc.Transaction, leafPageID uint32, parents []*buffer.Frame) error {
	if len(parents) == 0 {
		// Leaf is also root — nothing to do.
		return nil
	}

	leafFrame, err := bt.bufPool.FetchPage(leafPageID)
	if err != nil {
		for _, pf := range parents {
			bt.bufPool.UnpinPage(pf.PageID, false)
		}
		return nil
	}
	leaf := bt.bufPool.ReadPageData(leafFrame)

	if countLiveSlots(leaf) >= minLeafFill {
		// Not underflowing — just unpin.
		bt.bufPool.UnpinPage(leafPageID, false)
		for _, pf := range parents {
			bt.bufPool.UnpinPage(pf.PageID, false)
		}
		return nil
	}

	// Locate siblings via parent page.
	parentFrame := parents[len(parents)-1]
	parent := bt.bufPool.ReadPageData(parentFrame)
	ip := &InternalPage{parent}

	rightSibID, leftSibID, sepSlotRight, sepSlotLeaf := bt.findSiblingInfo(ip, leafPageID)

	// --- Try borrow from right sibling ---
	if rightSibID != 0 {
		rightFrame, rerr := bt.bufPool.FetchPage(rightSibID)
		if rerr == nil {
			right := bt.bufPool.ReadPageData(rightFrame)
			if canLend(right) {
				bt.doborrowFromRight(leaf, right, ip, sepSlotRight, rightSibID)
				bt.bufPool.WritePageData(leafPageID, leaf)
				bt.bufPool.WritePageData(rightSibID, right)
				bt.bufPool.WritePageData(parentFrame.PageID, parent)
				bt.bufPool.UnpinPage(leafPageID, true)
				bt.bufPool.UnpinPage(rightSibID, true)
				for _, pf := range parents {
					bt.bufPool.UnpinPage(pf.PageID, true)
				}
				if bt.bus != nil {
					bt.bus.Publish(events.Event{Type: events.EvtTreeMerge,
						Extra: map[string]interface{}{"op": "borrow_right", "leaf": leafPageID}})
				}
				return nil
			}
			bt.bufPool.UnpinPage(rightSibID, false)
		}
	}

	// --- Try borrow from left sibling ---
	if leftSibID != 0 {
		leftFrame, lerr := bt.bufPool.FetchPage(leftSibID)
		if lerr == nil {
			left := bt.bufPool.ReadPageData(leftFrame)
			if canLend(left) {
				bt.doborrowFromLeft(left, leaf, ip, sepSlotLeaf, leafPageID)
				bt.bufPool.WritePageData(leftSibID, left)
				bt.bufPool.WritePageData(leafPageID, leaf)
				bt.bufPool.WritePageData(parentFrame.PageID, parent)
				bt.bufPool.UnpinPage(leftSibID, true)
				bt.bufPool.UnpinPage(leafPageID, true)
				for _, pf := range parents {
					bt.bufPool.UnpinPage(pf.PageID, true)
				}
				if bt.bus != nil {
					bt.bus.Publish(events.Event{Type: events.EvtTreeMerge,
						Extra: map[string]interface{}{"op": "borrow_left", "leaf": leafPageID}})
				}
				return nil
			}
			bt.bufPool.UnpinPage(leftSibID, false)
		}
	}

	// --- Merge: prefer absorbing right sibling into leaf ---
	if rightSibID != 0 && sepSlotRight >= 0 {
		rightFrame, rerr := bt.bufPool.FetchPage(rightSibID)
		if rerr == nil {
			right := bt.bufPool.ReadPageData(rightFrame)
			return bt.doMergeWithRight(txn, leaf, leafPageID, right, rightSibID, ip, parent, parentFrame, parents)
		}
	}

	// --- Merge leaf into left sibling ---
	if leftSibID != 0 && sepSlotLeaf >= 0 {
		leftFrame, lerr := bt.bufPool.FetchPage(leftSibID)
		if lerr == nil {
			left := bt.bufPool.ReadPageData(leftFrame)
			return bt.doMergeIntoLeft(txn, left, leftSibID, leaf, leafPageID, ip, parent, parentFrame, parents, sepSlotLeaf)
		}
	}

	// Cannot borrow or merge — just unpin.
	bt.bufPool.UnpinPage(leafPageID, false)
	for _, pf := range parents {
		bt.bufPool.UnpinPage(pf.PageID, false)
	}
	return nil
}

// findSiblingInfo returns the right and left sibling IDs, and the separator slot indices
// in the parent that point to the right sibling (sepSlotRight) and to leafPageID (sepSlotLeaf).
// sepSlotLeaf == -1 means leaf is the leftmost child (PrevSib); sepSlotRight == -1 means no right sib.
func (bt *BPlusTree) findSiblingInfo(ip *InternalPage, leafPageID uint32) (rightSibID, leftSibID uint32, sepSlotRight, sepSlotLeaf int) {
	sepSlotRight = -1
	sepSlotLeaf = -1

	leftmostChild := ip.PrevSib()
	n := ip.NumSlots()

	if leftmostChild == leafPageID {
		// Leaf is the leftmost child — no left sibling in this subtree.
		if n > 0 {
			data := ip.GetRecord(0)
			if data != nil {
				r, _ := DecodeInternalRecord(data)
				rightSibID = r.ChildID
				sepSlotRight = 0
			}
		}
		return
	}

	for i := 0; i < n; i++ {
		data := ip.GetRecord(i)
		if data == nil {
			continue
		}
		r, _ := DecodeInternalRecord(data)
		if r.ChildID == leafPageID {
			sepSlotLeaf = i
			// Left sibling
			if i == 0 {
				leftSibID = leftmostChild
			} else {
				ld := ip.GetRecord(i - 1)
				if ld != nil {
					lr, _ := DecodeInternalRecord(ld)
					leftSibID = lr.ChildID
				}
			}
			// Right sibling
			if i+1 < n {
				rd := ip.GetRecord(i + 1)
				if rd != nil {
					rr, _ := DecodeInternalRecord(rd)
					rightSibID = rr.ChildID
					sepSlotRight = i + 1
				}
			}
			return
		}
	}
	return
}

// doborrowFromRight moves the first record from right sibling into leaf, updates parent separator.
func (bt *BPlusTree) doborrowFromRight(leaf, right *page.Page, ip *InternalPage, sepSlot int, rightSibID uint32) {
	first := right.GetRecord(0)
	if first == nil {
		return
	}
	cp := make([]byte, len(first))
	copy(cp, first)

	leaf.InsertRecord(cp, mvcc.TupleKeyExtract) //nolint
	right.DeleteRecord(0)
	right.Compact()

	// Update separator in parent at sepSlot: new separator = new first key of right.
	if right.NumSlots() > 0 {
		newFirst := right.GetRecord(0)
		if newFirst != nil {
			newKey := mvcc.TupleKeyExtract(newFirst)
			ip.DeleteRecord(sepSlot)
			ip.Compact()
			ip.InsertKey(newKey, rightSibID) //nolint
		}
	}
}

// doborrowFromLeft moves the last record from left sibling into leaf, updates parent separator.
func (bt *BPlusTree) doborrowFromLeft(left, leaf *page.Page, ip *InternalPage, sepSlotLeaf int, leafPageID uint32) {
	n := left.NumSlots()
	if n == 0 {
		return
	}
	last := left.GetRecord(n - 1)
	if last == nil {
		return
	}
	cp := make([]byte, len(last))
	copy(cp, last)

	leaf.InsertRecord(cp, mvcc.TupleKeyExtract) //nolint
	left.DeleteRecord(n - 1)
	left.Compact()

	// Update separator: new separator for leaf = its new minimum key.
	if leaf.NumSlots() > 0 {
		newFirst := leaf.GetRecord(0)
		if newFirst != nil {
			newKey := mvcc.TupleKeyExtract(newFirst)
			ip.DeleteRecord(sepSlotLeaf)
			ip.Compact()
			ip.InsertKey(newKey, leafPageID) //nolint
		}
	}
}

// doMergeWithRight absorbs all records from right into leaf, removes right from parent.
func (bt *BPlusTree) doMergeWithRight(txn *mvcc.Transaction, leaf *page.Page, leafPageID uint32,
	right *page.Page, rightSibID uint32,
	ip *InternalPage, parent *page.Page, parentFrame *buffer.Frame, parents []*buffer.Frame) error {

	// Copy all records from right into leaf.
	n := right.NumSlots()
	for i := 0; i < n; i++ {
		data := right.GetRecord(i)
		if data == nil {
			continue
		}
		cp := make([]byte, len(data))
		copy(cp, data)
		leaf.InsertRecord(cp, mvcc.TupleKeyExtract) //nolint
	}

	// Update sibling links.
	oldNextSib := right.NextSib()
	leaf.SetNextSib(oldNextSib)
	if oldNextSib != 0 {
		nextFrame, err := bt.bufPool.FetchPage(oldNextSib)
		if err == nil {
			nextPage := bt.bufPool.ReadPageData(nextFrame)
			nextPage.SetPrevSib(leafPageID)
			bt.bufPool.WritePageData(oldNextSib, nextPage)
			bt.bufPool.UnpinPage(oldNextSib, true)
		}
	}

	// Remove separator from parent that points to rightSibID.
	sep := ip.NumSlots()
	for i := 0; i < sep; i++ {
		data := ip.GetRecord(i)
		if data == nil {
			continue
		}
		r, _ := DecodeInternalRecord(data)
		if r.ChildID == rightSibID {
			ip.DeleteRecord(i)
			parent.Compact()
			break
		}
	}

	// WAL: log after-images of surviving leaf and parent before making pages dirty.
	if bt.walMgr != nil {
		lsn := bt.walMgr.LogMergeRecord(uint64(txn.ID), leafPageID, rightSibID, parentFrame.PageID,
			txn.LastLSN, leaf.Data[:], parent.Data[:])
		txn.LastLSN = lsn
		leaf.SetPageLSN(lsn)
		parent.SetPageLSN(lsn)
	}

	bt.bufPool.WritePageData(leafPageID, leaf)
	bt.bufPool.WritePageData(parentFrame.PageID, parent)
	bt.bufPool.UnpinPage(leafPageID, true)
	bt.bufPool.UnpinPage(rightSibID, false) // unpin before free
	bt.disk.FreePage(rightSibID)            //nolint
	bt.bufPool.InvalidatePage(rightSibID)   // prevent stale dirty frame from overwriting freelist

	// Bloom: discard freed page's filter, rebuild surviving leaf's filter.
	bt.bloom.Remove(rightSibID)
	bt.rebuildPageBloom(leafPageID, leaf)

	if bt.bus != nil {
		bt.bus.Publish(events.Event{Type: events.EvtPageMerge,
			Extra: map[string]interface{}{"left": leafPageID, "right": rightSibID}})
	}

	// Check if parent now underflows.
	if parent.NumSlots() == 0 {
		return bt.handleRootShrink(txn, leafPageID, parentFrame, parents)
	}
	for _, pf := range parents {
		bt.bufPool.UnpinPage(pf.PageID, true)
	}
	return nil
}

// doMergeIntoLeft copies leaf records into left sibling, removes leaf from parent.
func (bt *BPlusTree) doMergeIntoLeft(txn *mvcc.Transaction,
	left *page.Page, leftSibID uint32,
	leaf *page.Page, leafPageID uint32,
	ip *InternalPage, parent *page.Page, parentFrame *buffer.Frame, parents []*buffer.Frame,
	sepSlotLeaf int) error {

	// Copy all records from leaf into left.
	n := leaf.NumSlots()
	for i := 0; i < n; i++ {
		data := leaf.GetRecord(i)
		if data == nil {
			continue
		}
		cp := make([]byte, len(data))
		copy(cp, data)
		left.InsertRecord(cp, mvcc.TupleKeyExtract) //nolint
	}

	// Update sibling links.
	oldNextSib := leaf.NextSib()
	left.SetNextSib(oldNextSib)
	if oldNextSib != 0 {
		nextFrame, err := bt.bufPool.FetchPage(oldNextSib)
		if err == nil {
			nextPage := bt.bufPool.ReadPageData(nextFrame)
			nextPage.SetPrevSib(leftSibID)
			bt.bufPool.WritePageData(oldNextSib, nextPage)
			bt.bufPool.UnpinPage(oldNextSib, true)
		}
	}

	// Remove separator from parent at sepSlotLeaf.
	ip.DeleteRecord(sepSlotLeaf)
	parent.Compact()

	// WAL: log after-images of surviving left sibling and parent.
	if bt.walMgr != nil {
		lsn := bt.walMgr.LogMergeRecord(uint64(txn.ID), leftSibID, leafPageID, parentFrame.PageID,
			txn.LastLSN, left.Data[:], parent.Data[:])
		txn.LastLSN = lsn
		left.SetPageLSN(lsn)
		parent.SetPageLSN(lsn)
	}

	bt.bufPool.WritePageData(leftSibID, left)
	bt.bufPool.WritePageData(parentFrame.PageID, parent)
	bt.bufPool.UnpinPage(leftSibID, true)
	bt.bufPool.UnpinPage(leafPageID, false) // unpin before free
	bt.disk.FreePage(leafPageID)            //nolint
	bt.bufPool.InvalidatePage(leafPageID)   // prevent stale dirty frame from overwriting freelist

	// Bloom: discard freed page's filter, rebuild surviving sibling's filter.
	bt.bloom.Remove(leafPageID)
	bt.rebuildPageBloom(leftSibID, left)

	if bt.bus != nil {
		bt.bus.Publish(events.Event{Type: events.EvtPageMerge,
			Extra: map[string]interface{}{"left": leftSibID, "right": leafPageID}})
	}

	// Check if parent now underflows.
	if parent.NumSlots() == 0 {
		return bt.handleRootShrink(txn, leftSibID, parentFrame, parents)
	}
	for _, pf := range parents {
		bt.bufPool.UnpinPage(pf.PageID, true)
	}
	return nil
}

// handleRootShrink is called when an internal page has no more separators (only 1 child left).
// If the node is the root, the tree shrinks by one level.
// If it is not the root, we cascade the underflow upward via handleInternalUnderflow.
func (bt *BPlusTree) handleRootShrink(txn *mvcc.Transaction, onlyChildID uint32, emptyFrame *buffer.Frame, parents []*buffer.Frame) error {
	emptyPage := bt.bufPool.ReadPageData(emptyFrame)
	if emptyPage.Flags()&page.FlagIsRoot == 0 {
		// Not the root: cascade internal underflow upward.
		if len(parents) == 0 {
			// No grandparent — tree is inconsistent; just unpin.
			bt.bufPool.UnpinPage(emptyFrame.PageID, true)
			return nil
		}
		grandparentFrame := parents[len(parents)-1]
		higherAncestors := parents[:len(parents)-1]
		return bt.handleInternalUnderflow(txn, emptyFrame.PageID, onlyChildID, emptyFrame, grandparentFrame, higherAncestors)
	}

	// IS the root: promote only child as new root.
	childFrame, err := bt.bufPool.FetchPage(onlyChildID)
	if err != nil {
		for _, pf := range parents {
			bt.bufPool.UnpinPage(pf.PageID, true)
		}
		return nil
	}
	childPage := bt.bufPool.ReadPageData(childFrame)
	childPage.SetFlag(page.FlagIsRoot)
	if bt.walMgr != nil {
		payload := make([]byte, 4)
		binary.LittleEndian.PutUint32(payload, onlyChildID)
		rootLSN := bt.walMgr.AppendRecord(wal.LogRecord{
			Type:    wal.LogRootChange,
			PrevLSN: txn.LastLSN,
			Payload: payload,
		})
		_ = bt.walMgr.FlushUpTo(rootLSN)
		txn.LastLSN = rootLSN
		childPage.SetPageLSN(rootLSN)
	}
	bt.bufPool.WritePageData(onlyChildID, childPage)
	bt.bufPool.UnpinPage(onlyChildID, true)

	bt.bufPool.UnpinPage(emptyFrame.PageID, false)
	bt.disk.FreePage(emptyFrame.PageID) //nolint
	bt.bufPool.InvalidatePage(emptyFrame.PageID)

	bt.rootPageID = onlyChildID
	if bt.onRootChange != nil {
		bt.onRootChange(onlyChildID)
	}

	for _, pf := range parents[:len(parents)-1] {
		bt.bufPool.UnpinPage(pf.PageID, false)
	}
	return nil
}

// handleInternalUnderflow is called when an internal node has been emptied (0 separators, 1 child).
// It merges the underflowing node with a sibling by pulling the grandparent separator down.
func (bt *BPlusTree) handleInternalUnderflow(
	txn *mvcc.Transaction,
	underflowID uint32,
	onlyChildID uint32,
	underflowFrame *buffer.Frame,
	grandparentFrame *buffer.Frame,
	ancestors []*buffer.Frame,
) error {
	gp := bt.bufPool.ReadPageData(grandparentFrame)
	gpIP := &InternalPage{gp}

	// Find underflowID's siblings in the grandparent.
	rightSibID, leftSibID, sepSlotRight, sepSlotUnderflow := bt.findSiblingInfo(gpIP, underflowID)

	// Try merge with right sibling first.
	if rightSibID != 0 && sepSlotRight >= 0 {
		rightFrame, err := bt.bufPool.FetchPage(rightSibID)
		if err == nil {
			right := bt.bufPool.ReadPageData(rightFrame)
			rightIP := &InternalPage{right}

			// Pull grandparent separator down: {gpSepKey → rightSibID} becomes first sep of merged node.
			gpSepData := gp.GetRecord(sepSlotRight)
			if gpSepData == nil {
				bt.bufPool.UnpinPage(rightSibID, false)
			} else {
				gpSepRec, _ := DecodeInternalRecord(gpSepData)
				gpSepKey := gpSepRec.Key

				// Build merged right: [onlyChildID] [gpSepKey → oldRightLeftmost] [old right seps...]
				oldRightLeftmost := rightIP.LeftmostChild()
				rightIP.SetLeftmostChild(onlyChildID)
				rightIP.InsertKey(gpSepKey, oldRightLeftmost) //nolint

				// Update grandparent: remove {gpSepKey, rightSibID} slot.
				for i := 0; i < gp.NumSlots(); i++ {
					d := gp.GetRecord(i)
					if d == nil {
						continue
					}
					r, _ := DecodeInternalRecord(d)
					if r.ChildID == rightSibID {
						gp.DeleteRecord(i)
						gp.Compact()
						break
					}
				}
				// Redirect the pointer TO underflowID → now points to rightSibID.
				if gp.PrevSib() == underflowID {
					gp.SetPrevSib(rightSibID)
				} else if sepSlotUnderflow >= 0 {
					// Update slot[sepSlotUnderflow].ChildID = rightSibID
					d := gp.GetRecord(sepSlotUnderflow)
					if d != nil {
						rec, _ := DecodeInternalRecord(d)
						gp.DeleteRecord(sepSlotUnderflow)
						gp.Compact()
						newRec := EncodeInternalRecord(InternalRecord{Key: rec.Key, ChildID: rightSibID})
						gp.InsertRecord(newRec, InternalKeyExtract) //nolint
					}
				}

				bt.bufPool.WritePageData(rightSibID, right)
				bt.bufPool.WritePageData(grandparentFrame.PageID, gp)
				bt.bufPool.UnpinPage(rightSibID, true)
				bt.bufPool.UnpinPage(underflowFrame.PageID, false)
				bt.disk.FreePage(underflowID) //nolint
				bt.bufPool.InvalidatePage(underflowID)

				// Recurse if grandparent now underflows.
				if gp.NumSlots() == 0 {
					return bt.handleRootShrink(txn, rightSibID, grandparentFrame, ancestors)
				}
				for _, pf := range ancestors {
					bt.bufPool.UnpinPage(pf.PageID, true)
				}
				return nil
			}
		}
	}

	// Try merge with left sibling.
	if leftSibID != 0 && sepSlotUnderflow >= 0 {
		leftFrame, err := bt.bufPool.FetchPage(leftSibID)
		if err == nil {
			left := bt.bufPool.ReadPageData(leftFrame)
			leftIP := &InternalPage{left}

			// Pull grandparent separator at sepSlotUnderflow down into left.
			gpSepData := gp.GetRecord(sepSlotUnderflow)
			if gpSepData != nil {
				gpSepRec, _ := DecodeInternalRecord(gpSepData)
				gpSepKey := gpSepRec.Key

				// Append to left: [existing left seps...] [gpSepKey → onlyChildID]
				leftIP.InsertKey(gpSepKey, onlyChildID) //nolint

				// Remove separator at sepSlotUnderflow from grandparent.
				gp.DeleteRecord(sepSlotUnderflow)
				gp.Compact()

				bt.bufPool.WritePageData(leftSibID, left)
				bt.bufPool.WritePageData(grandparentFrame.PageID, gp)
				bt.bufPool.UnpinPage(leftSibID, true)
				bt.bufPool.UnpinPage(underflowFrame.PageID, false)
				bt.disk.FreePage(underflowID) //nolint
				bt.bufPool.InvalidatePage(underflowID)

				if gp.NumSlots() == 0 {
					return bt.handleRootShrink(txn, leftSibID, grandparentFrame, ancestors)
				}
				for _, pf := range ancestors {
					bt.bufPool.UnpinPage(pf.PageID, true)
				}
				return nil
			}
			bt.bufPool.UnpinPage(leftSibID, false)
		}
	}

	// Cannot merge — leave slightly underflowing.
	bt.bufPool.UnpinPage(underflowFrame.PageID, true)
	bt.bufPool.UnpinPage(grandparentFrame.PageID, true)
	for _, pf := range ancestors {
		bt.bufPool.UnpinPage(pf.PageID, true)
	}
	return nil
}

// countLiveSlots counts the number of non-tombstone slots in the page.
func countLiveSlots(p *page.Page) int {
	n := p.NumSlots()
	count := 0
	for i := 0; i < n; i++ {
		if p.GetRecord(i) != nil {
			count++
		}
	}
	return count
}

// canLend returns true if a sibling has enough records to spare one.
func canLend(p *page.Page) bool {
	return countLiveSlots(p) > minLeafFill
}

// Rebalance scans all leaf pages (via sibling links from the leftmost leaf) and
// merges any that have fallen below minLeafFill after compaction.
// Call this after VacuumWorker.RunOnce() to consolidate sparsely-populated pages.
func (bt *BPlusTree) Rebalance(txn *mvcc.Transaction) error {
	// Walk from the leftmost leaf.
	rootFrame, err := bt.bufPool.FetchPage(bt.rootPageID)
	if err != nil {
		return nil
	}
	p := bt.bufPool.ReadPageData(rootFrame)
	for p.Type() == page.PageTypeInternal {
		childID := p.PrevSib()
		bt.bufPool.UnpinPage(rootFrame.PageID, false)
		if childID == 0 {
			return nil
		}
		rootFrame, err = bt.bufPool.FetchPage(childID)
		if err != nil {
			return nil
		}
		p = bt.bufPool.ReadPageData(rootFrame)
	}
	startLeafID := rootFrame.PageID
	bt.bufPool.UnpinPage(startLeafID, false)

	// Iterate leaf pages; re-acquire key for traversal.
	leafID := startLeafID
	for leafID != 0 {
		leafFrame, err := bt.bufPool.FetchPage(leafID)
		if err != nil {
			break
		}
		leaf := bt.bufPool.ReadPageData(leafFrame)
		nextID := leaf.NextSib()
		live := countLiveSlots(leaf)
		bt.bufPool.UnpinPage(leafID, false)

		if live < minLeafFill {
			// Re-find the leaf with its parent chain.
			var searchKey []byte
			if live > 0 {
				// Use any live key from this leaf to navigate to it.
				lf2, err := bt.bufPool.FetchPage(leafID)
				if err == nil {
					p2 := bt.bufPool.ReadPageData(lf2)
					for i := 0; i < p2.NumSlots(); i++ {
						d := p2.GetRecord(i)
						if d != nil {
							searchKey = mvcc.TupleKeyExtract(d)
							break
						}
					}
					bt.bufPool.UnpinPage(leafID, false)
				}
			}
			if searchKey == nil {
				// Empty page — try left-bias scan for a key from the left sibling.
				leafID = nextID
				continue
			}

			frame2, parents2, err := bt.findLeafWithParents(searchKey)
			if err != nil {
				leafID = nextID
				continue
			}
			// Make sure we landed on the same leaf.
			if frame2.PageID != leafID {
				bt.bufPool.UnpinPage(frame2.PageID, false)
				for _, pf := range parents2 {
					bt.bufPool.UnpinPage(pf.PageID, false)
				}
				leafID = nextID
				continue
			}
			bt.bufPool.UnpinPage(frame2.PageID, false)
			if err := bt.handleUnderflow(txn, leafID, parents2); err != nil {
				return err
			}
		}

		leafID = nextID
	}
	return nil
}

// ensure bytes import is used
var _ = bytes.Compare
