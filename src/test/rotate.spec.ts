import { readFileSync, createReadStream } from "node:fs";
import path from "node:path";
import test from "ava";
import { transformCSVStream } from "../rotate.js";
import { EOL } from "node:os";

for (const dataFileName of ["sample", "data"]) {
  test(`Data File '${dataFileName}'`, async (t) => {
    const transformedChunks = await new Promise<readonly string[]>(
      (resolve) => {
        const chunks: string[] = [];

        transformCSVStream(
          createReadStream(path.join("fixtures", `${dataFileName}.input`), {
            encoding: "utf8",
            autoClose: true,
            flags: "r",
          })
        )
          .on("data", (data) => {
            chunks.push(data + EOL);
          })
          .on("end", () => {
            resolve(chunks);
          });
      }
    );

    t.deepEqual(
      transformedChunks.join(''),
      readFileSync(path.join("fixtures", `${dataFileName}.output`), {
        encoding: "utf8",
        flag: "r",
      })
    );
  });
}
