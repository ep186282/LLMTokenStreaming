export interface SseFrame {
  data: string;
  event?: string;
  id?: string;
}

export class SseParser {
  private readonly decoder = new TextDecoder();
  private textBuffer = "";
  private dataLines: string[] = [];
  private eventName: string | undefined;
  private eventId: string | undefined;
  private hasFields = false;

  push(chunk: Uint8Array): SseFrame[] {
    this.textBuffer += this.decoder.decode(chunk, { stream: true });
    return this.drainCompleteLines();
  }

  finish(): SseFrame[] {
    this.textBuffer += this.decoder.decode();
    const frames = this.drainCompleteLines();
    // EOF discards a frame that was never terminated by a blank line.
    this.textBuffer = "";
    this.resetFrame();
    return frames;
  }

  reset(): void {
    this.decoder.decode();
    this.textBuffer = "";
    this.resetFrame();
  }

  private drainCompleteLines(): SseFrame[] {
    const frames: SseFrame[] = [];
    let newlineIndex = this.textBuffer.indexOf("\n");

    while (newlineIndex !== -1) {
      const line = this.stripTrailingCarriageReturn(
        this.textBuffer.slice(0, newlineIndex),
      );
      this.textBuffer = this.textBuffer.slice(newlineIndex + 1);
      this.processLine(line, frames);
      newlineIndex = this.textBuffer.indexOf("\n");
    }

    return frames;
  }

  private processLine(line: string, frames: SseFrame[]): void {
    if (line === "") {
      this.dispatch(frames);
      return;
    }

    if (line.startsWith(":")) {
      return;
    }

    const colonIndex = line.indexOf(":");
    const field = colonIndex === -1 ? line : line.slice(0, colonIndex);
    let value = colonIndex === -1 ? "" : line.slice(colonIndex + 1);

    if (value.startsWith(" ")) {
      value = value.slice(1);
    }

    switch (field) {
      case "data":
        this.dataLines.push(value);
        this.hasFields = true;
        break;
      case "event":
        this.eventName = value;
        this.hasFields = true;
        break;
      case "id":
        if (!value.includes("\0")) {
          this.eventId = value;
          this.hasFields = true;
        }
        break;
      default:
        break;
    }
  }

  private dispatch(frames: SseFrame[]): void {
    if (!this.hasFields) {
      this.resetFrame();
      return;
    }

    frames.push({
      data: this.dataLines.join("\n"),
      ...(this.eventName !== undefined ? { event: this.eventName } : {}),
      ...(this.eventId !== undefined ? { id: this.eventId } : {}),
    });
    this.resetFrame();
  }

  private resetFrame(): void {
    this.dataLines = [];
    this.eventName = undefined;
    this.eventId = undefined;
    this.hasFields = false;
  }

  private stripTrailingCarriageReturn(value: string): string {
    return value.endsWith("\r") ? value.slice(0, -1) : value;
  }
}
