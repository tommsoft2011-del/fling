import { replaceChildrenPreservingScroll } from '../src/dom-utils.js';

function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(`FAIL: ${message}`);
  console.log(`ok - ${message}`);
}

for (const view of ['send', 'receive'] as const) {
  const container = {
    scrollTop: 73,
    children: [] as unknown[],
    replaceChildren(...children: unknown[]) {
      this.children = children;
      // Simulate a browser resetting the scroll offset after a full child refresh.
      this.scrollTop = 0;
    },
  };
  for (let progress = 1; progress <= 3; progress++) {
    replaceChildrenPreservingScroll(
      container as unknown as HTMLElement,
      [{ view, progress }] as unknown as Node[],
    );
    check(container.scrollTop === 73, `${view} detail keeps scrollTop during progress refresh ${progress}`);
    check(container.children.length === 1, `${view} detail updates rendered rows during refresh ${progress}`);
  }
}
