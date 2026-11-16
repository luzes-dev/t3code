import { ORCHESTRATION_PROTOCOL_VERSION } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { sandboxSocketUrl } from "./sandboxClient.ts";

describe("sandboxSocketUrl", () => {
  it("serves RPC at /ws with the ticket and the orchestration protocol it speaks", () => {
    const url = new URL(sandboxSocketUrl("https://box.example.test", "ticket-1"));
    expect(url.protocol).toBe("wss:");
    expect(url.pathname).toBe("/ws");
    expect(url.searchParams.get("wsTicket")).toBe("ticket-1");
    // A sandbox refuses the upgrade (426) from a client that does not name its protocol.
    expect(url.searchParams.get("orchestrationProtocol")).toBe(
      String(ORCHESTRATION_PROTOCOL_VERSION),
    );
  });

  it("keeps a path the address already has", () => {
    const url = new URL(sandboxSocketUrl("http://127.0.0.1:3773/rpc", "t"));
    expect(url.protocol).toBe("ws:");
    expect(url.pathname).toBe("/rpc");
  });
});
