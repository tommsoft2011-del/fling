export interface NativeSourceMetadata { sourceId: string; name: string; size: number }
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
