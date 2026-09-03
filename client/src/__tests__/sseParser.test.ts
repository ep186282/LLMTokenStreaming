import { describe, expect, it } from "vitest";
import { SseParser, type SseFrame } from "../sseParser";

const encoder = new TextEncoder();

function parseChunks(chunks: Uint8Array[]): SseFrame[] {
  const parser = new SseParser();
  const frames = chunks.flatMap((chunk) => parser.push(chunk));
  frames.push(...parser.finish());
  return frames;
}

describe("SseParser", () => {
  it("parses the same frames when split at every byte offset", () => {
    const source = [
      ": heartbeat",
      "",
      "id: 17",
      "event: chunk",
      'data: {"type":"text.delta",',
      'data: "text":"hello"}',
      "",
      "id: 19",
      "event: done",
      'data: {"final_seq":17}',
      "",
      "",
    ].join("\n");
    const bytes = encoder.encode(source);
    const expected: SseFrame[] = [
      {
        id: "17",
        event: "chunk",
        data: '{"type":"text.delta",\n"text":"hello"}',
      },
      {
        id: "19",
        event: "done",
        data: '{"final_seq":17}',
      },
    ];

    for (let split = 0; split <= bytes.length; split += 1) {
      expect(
        parseChunks([bytes.slice(0, split), bytes.slice(split)]),
      ).toEqual(expected);
    }
  });

  it("preserves an emoji split inside its UTF-8 bytes", () => {
    const source = 'event: chunk\ndata: {"text":"A🧠B"}\n\n';
    const bytes = encoder.encode(source);
    const emojiBytes = encoder.encode("🧠");
    const emojiStart = bytes.findIndex(
      (_, index) =>
        bytes
          .slice(index, index + emojiBytes.length)
          .every((byte, byteIndex) => byte === emojiBytes[byteIndex]),
    );

    expect(emojiStart).toBeGreaterThan(0);
    const frames = parseChunks([
      bytes.slice(0, emojiStart + 1),
      bytes.slice(emojiStart + 1, emojiStart + 3),
      bytes.slice(emojiStart + 3),
    ]);

    expect(frames).toEqual([
      {
        event: "chunk",
        data: '{"text":"A🧠B"}',
      },
    ]);
  });

  it.each([
    ["LF", "\n"],
    ["CRLF", "\r\n"],
  ])("handles %s line endings", (_, newline) => {
    const source = [
      "id: 4",
      "event: chunk",
      "data: first",
      "data: second",
      "",
      "",
    ].join(newline);

    expect(parseChunks([encoder.encode(source)])).toEqual([
      {
        id: "4",
        event: "chunk",
        data: "first\nsecond",
      },
    ]);
  });

  it("joins multiple data fields with a newline", () => {
    const frames = parseChunks([
      encoder.encode("data: one\ndata: two\ndata: three\n\n"),
    ]);

    expect(frames).toEqual([{ data: "one\ntwo\nthree" }]);
  });

  it("ignores comment-only frames", () => {
    const frames = parseChunks([
      encoder.encode(": keep-alive\n\n: another heartbeat\n\n"),
    ]);

    expect(frames).toEqual([]);
  });

  it("discards a frame that is incomplete at EOF", () => {
    const frames = parseChunks([
      encoder.encode(
        "id: 1\nevent: chunk\ndata: complete\n\n" +
          'id: 2\nevent: chunk\ndata: {"text":"partial"}',
      ),
    ]);

    expect(frames).toEqual([
      { id: "1", event: "chunk", data: "complete" },
    ]);
  });

  it("handles a stream delivered one byte at a time", () => {
    const bytes = encoder.encode("id: 1\nevent: chunk\ndata: ok\n\n");

    expect(parseChunks(Array.from(bytes, (byte) => Uint8Array.of(byte)))).toEqual(
      [{ id: "1", event: "chunk", data: "ok" }],
    );
  });
});
