// Transport abstraction: FlingRoom's E2E/presence/file logic is identical
// everywhere; only the LiveKit binding differs.
//   - Node/Bun CLI  -> transports/node.ts  (@livekit/rtc-node)
//   - Browser/Tauri -> implemented in the app (@livekit/livekit-client)

export type DataHandler = (payload: Uint8Array, from: string, topic: string) => void;

export interface FlingTransport {
  readonly isConnected: boolean;
  /** Our own participant identity (valid after connect). */
  readonly localIdentity: string;
  connect(url: string, token: string): Promise<void>;
  disconnect(): Promise<void>;
  /** Publish a payload on the Fling topic (reliable, encrypted by caller). */
  publish(payload: Uint8Array, topic: string): Promise<void>;
  onData(handler: DataHandler): void;
  onPeerJoined(handler: (identity: string) => void): void;
  onPeerLeft(handler: (identity: string) => void): void;
}
