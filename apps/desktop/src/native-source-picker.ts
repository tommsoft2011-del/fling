export interface NativeSourceMetadata { sourceId: string; name: string; size: number; relPath?: string; folder?: string }
/** UI-only selection guard. Selection, authorization and handle ownership stay in Rust. */
export class NativeSourcePicker {
  private busy = false;
  constructor(private readonly select: () => Promise<NativeSourceMetadata | null>) {}
  async pick(): Promise<NativeSourceMetadata | null> {
    if (this.busy) return null;
    this.busy = true;
    try { return await this.select(); }
    finally { this.busy = false; }
  }
}

/** Batch UI-only guard. Cancellation/error always releases the picker lock. */
export class NativeSourcesPicker {
  private busy = false;
  constructor(private readonly select: (folders: boolean) => Promise<NativeSourceMetadata[]>) {}
  async pick(folders = false): Promise<NativeSourceMetadata[]> {
    if (this.busy) return [];
    this.busy = true;
    try { return await this.select(folders); }
    finally { this.busy = false; }
  }
}
