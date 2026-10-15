/** Replace rendered children without jumping a scrollable detail panel to the top. */
export function replaceChildrenPreservingScroll(container: HTMLElement, children: readonly Node[]): void {
  const scrollTop = container.scrollTop;
  container.replaceChildren(...children);
  container.scrollTop = scrollTop;
}
