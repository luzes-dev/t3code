/**
 * The images a sandbox agent's reply embeds by a path on the sandbox, which the host's clients
 * would resolve on the host and not find. The bridge copies each to the host and points the
 * reply at the copy, so text naming one waits until the reference is whole.
 */
import { PROVIDER_SEND_TURN_MAX_IMAGE_BYTES } from "@t3tools/contracts";
import { hostImageDestination } from "@t3tools/shared/generatedImageMarkdown";
import {
  markdownDestinationHostPath,
  normalizeMarkdownLinkDestination,
} from "@t3tools/shared/markdownLinks";
import { chatMarkdownImageSpans } from "@t3tools/shared/markdownPipeline";

/** Larger images stay on the sandbox; the reply keeps its path. */
export const RELAYED_IMAGE_MAX_BYTES = PROVIDER_SEND_TURN_MAX_IMAGE_BYTES;
/** The types T3 takes as chat images; anything else stays on the sandbox. */
export const RELAYED_IMAGE_TYPES: Readonly<Record<string, string>> = {
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".webp": "image/webp",
};

export interface RelayedImage {
  readonly bytes: Uint8Array;
  /** A key of `RELAYED_IMAGE_TYPES`. */
  readonly extension: string;
}

export const relayedImageExtension = (path: string): string | undefined => {
  const extension = /\.[a-z0-9]+$/i.exec(path)?.[0]?.toLowerCase();
  return extension !== undefined && Object.hasOwn(RELAYED_IMAGE_TYPES, extension)
    ? extension
    : undefined;
};

const MARKDOWN_IMAGE =
  /!\[(?:[^\]\\\n]|\\.)*\]\(\s*(?<destination><[^>\n]*>|[^\s()<>]+(?:\([^\s()]*\)[^\s()<>]*)*)/dg;
const HTML_IMAGE = /<img\b[^>]*?\ssrc\s*=\s*(?<value>"[^"]*"|'[^']*'|[^\s"'>]+)/dgi;
const MARKDOWN_IMAGE_REST =
  /^\s*(?:<[^>\n]*>|[^\s()<>]*(?:\([^\s()]*\)[^\s()<>]*)*)(?:\s+(?:"[^"\n]*"|'[^'\n]*'|\([^)\n]*\)))?\s*\)/;
const REFERENCE_START = /!\[|!$|<i?m?g?$|<img\b/gi;

/** Where the reference at the start of `rest` ends, or null while it may still grow. */
const referenceEnd = (rest: string): number | null => {
  if (rest.startsWith("<")) {
    if (/^<(?:i|im|img)?$/i.test(rest)) return null;
    const close = rest.indexOf(">");
    return close < 0 ? null : close + 1;
  }
  if (rest === "!") return null;
  const alt = /^!\[(?:[^\]\\\n]|\\.)*\\?/.exec(rest)![0];
  if (alt.length === rest.length) return null;
  const afterAlt = alt.length + 1;
  if (afterAlt === rest.length) return null;
  if (rest[afterAlt] !== "(") return afterAlt;
  const destination = MARKDOWN_IMAGE_REST.exec(rest.slice(afterAlt + 1));
  return destination === null ? null : afterAlt + 1 + destination[0].length;
};

/**
 * How much of `text` after `from` can go out now: all but an image reference still arriving on
 * the last line. A reference never spans lines, so a finished line always goes.
 */
export const releasableEnd = (text: string, from: number): number => {
  const lineStart = Math.max(from, text.lastIndexOf("\n") + 1);
  const line = text.slice(lineStart);
  REFERENCE_START.lastIndex = 0;
  let match: RegExpExecArray | null;
  while ((match = REFERENCE_START.exec(line)) !== null) {
    const end = referenceEnd(line.slice(match.index));
    if (end === null) return lineStart + match.index;
    REFERENCE_START.lastIndex = match.index + end;
  }
  return text.length;
};

export interface ImageReference {
  /** Where the destination, as written, sits in the text. */
  readonly start: number;
  readonly end: number;
  /** The path on the sandbox, absolute or relative to its workspace, as its renderer reads it. */
  readonly path: string;
  readonly form: "markdown" | "html";
}

/** A destination the sandbox's renderer loads from its own disk, as the path to read there. */
const sandboxImagePath = (source: string): string | null => {
  if (source === "" || /^(?:[#?~]|\/\/)/.test(source)) return null;
  if (/^[a-z][a-z0-9+.-]*:/i.test(source) && !/^file:/i.test(source)) return null;
  const path = markdownDestinationHostPath(source);
  return path === null || path === "" || relayedImageExtension(path) === undefined ? null : path;
};

/**
 * The local images `text` shows between `from` and `to`, found where chat markdown renders an
 * image, so one in code or a plain link to one is left alone.
 */
export const localImageReferences = (
  text: string,
  from: number,
  to: number,
): ReadonlyArray<ImageReference> => {
  const segment = text.slice(from, to);
  const written: Array<Omit<ImageReference, "path"> & { readonly raw: string }> = [];
  for (const match of segment.matchAll(MARKDOWN_IMAGE)) {
    const [start, end] = match.indices!.groups!.destination!;
    const raw = match.groups!.destination!;
    written.push({ start: from + start, end: from + end, raw, form: "markdown" });
  }
  for (const match of segment.matchAll(HTML_IMAGE)) {
    const [start, end] = match.indices!.groups!.value!;
    const raw = match.groups!.value!.replace(/^(["'])(.*)\1$/s, "$2");
    written.push({ start: from + start, end: from + end, raw, form: "html" });
  }
  const local = written.flatMap((candidate) => {
    const source = normalizeMarkdownLinkDestination(candidate.raw);
    const path = sandboxImagePath(source);
    return path === null ? [] : [{ ...candidate, source, path }];
  });
  if (local.length === 0) return [];
  const spans = chatMarkdownImageSpans(text.slice(0, to));
  return local
    .filter((candidate) =>
      spans.some(
        (span) =>
          span.start <= candidate.start &&
          candidate.end <= span.end &&
          markdownDestinationHostPath(span.source) ===
            markdownDestinationHostPath(candidate.source),
      ),
    )
    .map(({ start, end, path, form }) => ({ start, end, path, form }))
    .sort((left, right) => left.start - right.start);
};

/** `segment` (starting at `offset` in its text) with each reference pointed at its host copy. */
export const pointAtHostCopies = (
  segment: string,
  offset: number,
  references: ReadonlyArray<ImageReference>,
  copies: ReadonlyMap<string, string>,
): string => {
  let result = "";
  let cursor = 0;
  for (const reference of references) {
    const hostPath = copies.get(reference.path);
    if (hostPath === undefined) continue;
    const destination = hostImageDestination(hostPath);
    result += segment.slice(cursor, reference.start - offset);
    result +=
      reference.form === "markdown"
        ? `<${destination.replace(/[<>]/g, "\\$&")}>`
        : `"${destination.replaceAll('"', "&quot;")}"`;
    cursor = reference.end - offset;
  }
  return result + segment.slice(cursor);
};
