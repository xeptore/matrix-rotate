import { createInterface } from "node:readline";
import { PassThrough } from "node:stream";
import chunk from "lodash.chunk";

type Matrix = readonly (readonly string[])[];

interface ParseMatrixResult {
  readonly unwrap: () => { readonly isValid: boolean; readonly json: string };
  readonly map: (mapper: (matrix: Matrix) => Matrix) => ParseMatrixResult;
}

function InvalidMatrix(): ParseMatrixResult {
  return {
    unwrap() {
      return { isValid: false, json: "[]" };
    },
    map() {
      return InvalidMatrix();
    },
  };
}

function ValidMatrix(matrix: Matrix): ParseMatrixResult {
  return {
    unwrap() {
      return { isValid: true, json: `[${matrix.flat(1).map(x => JSON.stringify(x)).join(', ')}]` };
    },
    map(mapper) {
      return ValidMatrix(mapper(matrix));
    },
  };
}

function parseMatrix(raw: string): ParseMatrixResult {
  try {
    const json = JSON.parse(raw);
    if (!Array.isArray(json)) {
      return InvalidMatrix();
    }

    const matrixEdgeLen = Math.sqrt(json.length);
    if (!Number.isInteger(matrixEdgeLen)) {
      return InvalidMatrix();
    }

    return ValidMatrix(chunk(json, matrixEdgeLen));
  } catch (error) {
    return InvalidMatrix();
  }
}

function rotate(matrix: Matrix): Matrix {
  let top = 0;
  let right = matrix.length - 1;
  let bottom = matrix.length - 1;
  let left = 0;

  const output = Array.from(matrix, (r) => Array.from(r));

  while (left < right && top < bottom) {
    let previous = matrix[top + 1]![left]!;

    for (let i = left; i <= right; i++) {
      const curr = matrix[top]![i]!;
      output[top]![i] = previous;
      previous = curr;
    }
    top++;

    for (let i = top; i <= bottom; i++) {
      const curr = matrix[i]![right]!;
      output[i]![right] = previous;
      previous = curr;
    }
    right--;

    for (let i = right; i >= left; i -= 1) {
      const curr = matrix[bottom]![i]!;
      output[bottom]![i] = previous;
      previous = curr;
    }
    bottom--;

    for (let i = bottom; i >= top; i -= 1) {
      const curr = matrix[i]![left]!;
      output[i]![left] = previous;
      previous = curr;
    }
    left++;
  }

  return output;
}

/**
 * Transforms input CSV `stream` line by line, and emits transformed CSV lines to the returned stream.
 *
 * Output emitted lines are **not** terminated with `EOL`, and _you should handle that by yourself_. It just emits transformed CSV line by line on `data` event.
 *
 * Upon closing the input `stream`, the output stream will be closed, and you can listen to the returned stream's `close` event, if you need to.
 *
 * The following assumptions are made:
 * - The first occurrence of comma (`,`) in CSV lines separates the `id`, and `json` fields data.
 * - Any invalid/unparsable JSON field will result in an invalid JSON.
 * - Any JSON field which cannot be parsed to an array will result in an invalid JSON.
 * - Only JSON array values which will result in a square shaped matrix are processed.
 * - Transformed serialized JSON array values are separated with comma-spaces (`, `) as specified in the challenge requirement samples.
 * - Resulting transformed JSON arrays are serialized using `JSON.stringify` function.
 *
 * @example
 *
 * // This will read CSV lines line-by-line from `data.txt` file,
 * // and outputs every transformed CSV line to stdout.
 * // Note that due to the fact that `console.log` includes an EOL in every call,
 * // there's no need to manually handle that. In other cases, e.g., directly writing to a file,
 * // you'll need to append an EOL on every `data` chunk you receive from the stream.
 * transformCSVStream(
 *   createReadStream("data.txt", { encoding: "utf8", autoClose: true, flags: "r" })
 * )
 * .on("data", (data) => {
 *   console.log(data);
 * });
 */
export function transformCSVStream(
  stream: NodeJS.ReadableStream
): NodeJS.ReadableStream {
  const rl = createInterface({
    input: stream,
    crlfDelay: Infinity,
  });

  const outStream = new PassThrough({
    decodeStrings: false,
    encoding: "utf8",
    defaultEncoding: "utf8",
  });

  function transformCsvDataLine(line: string) {
    const indexOfComma = line.indexOf(",");
    const id = line.slice(0, indexOfComma);
    const json = line.slice(indexOfComma + 1).replaceAll(/^"|"$/g, "");
    const result = parseMatrix(json).map(rotate).unwrap();
    outStream.write(
      [id, `"${result.json}"`, result.isValid].join(",")
    );
  }

  rl.once("line", () => {
    outStream.write("id,json,is_valid");
    rl.on("line", transformCsvDataLine);
  });

  rl.on("close", () => outStream.end());

  return outStream;
}
