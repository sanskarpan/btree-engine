const PAGE_SELECTED_EVENT = "btree:page-selected";

export function selectPage(pageID: number) {
  if (typeof window === "undefined") {
    return;
  }
  window.dispatchEvent(new CustomEvent<number>(PAGE_SELECTED_EVENT, { detail: pageID }));
}

export function subscribeToPageSelection(handler: (pageID: number) => void): () => void {
  if (typeof window === "undefined") {
    return () => {};
  }
  const listener = (event: Event) => {
    const customEvent = event as CustomEvent<number>;
    handler(customEvent.detail);
  };
  window.addEventListener(PAGE_SELECTED_EVENT, listener);
  return () => window.removeEventListener(PAGE_SELECTED_EVENT, listener);
}
