// Mint a LiveKit access token locally using the API key/secret.
// The secret never leaves this machine; it only signs the JWT.

import { AccessToken } from 'livekit-server-sdk';

export async function mintToken(
  apiKey: string,
  apiSecret: string,
  roomName: string,
  identity: string,
): Promise<string> {
  const at = new AccessToken(apiKey, apiSecret, { identity, name: identity });
  at.addGrant({
    roomJoin: true,
    room: roomName,
    canPublish: true,
    canSubscribe: true,
    canPublishData: true,
  });
  return await at.toJwt();
}
