const fs = require('fs');
const os = require('os');
const path = require('path');
const { FileUtil } = require('../lib/FileUtil.js');

function concatUint8(chunks) {
  const total = chunks.reduce((n, c) => n + c.length, 0);
  const out = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) {
    out.set(c, off);
    off += c.length;
  }
  return out;
}

function makeTestFile({ fullSegments, segSize, remainder }) {
  const p = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'fu-')), 'blob.bin');
  const full = Buffer.alloc(segSize, 0xAB);
  const rem  = Buffer.alloc(remainder, 0xCD);

  const fd = fs.openSync(p, 'w');
  try {
    for (let i = 0; i < fullSegments; i++) fs.writeSync(fd, full);
    fs.writeSync(fd, rem);
  } finally {
    fs.closeSync(fd);
  }

  const original = fs.readFileSync(p);
  return { filePath: p, original };
}

describe('FileUtil EOF/read race', () => {
  test('handles data in stream buffer when EOF fires during backpressure', async () => {
    // This test is focused on the handling of the 'end' EOF event and how it
    // iteracts with ongoing or delayed reads from the buffered data and when it
    // finalizes or signals completion of the reading process to the consumer. 
    //
    // A critical requirement is that finalization must not occur until all
    // buffered data has been drained and delivered to the consumer.
    //
    // One way to fail this requirement would be if the 'end' handler relies on
    // a part count comparisons like part === totalParts to decide when to
    // finalize, and the predicted totalParts is calculated incorrectly. For
    // example if it were to assume that the first part is 1/4 the size of other
    // parts when calculating the total part count when in actuality the
    // chunking logic creates a full-sized first part, then for some file sizes
    // i.e. 3/4 of file sizes where the extra capacity of a full-size first part
    // reduced the total part count by 1, it would be incorrect.
    //
    // Such an incorrect finalization decision in the 'end' handler could then
    // be problematic if the 'end' event fires when there is undrained data.
    //
    // In FileUtil, the end handler exhibits this problematic behavior:
    //  * When backpressure prevents read() calls, reading=false
    //  * totalParts is calculated assuming first part is 1/4 size, but processChunk
    //    actually makes all parts full size, causing totalParts to overestimate by 1
    //    in ~75% of cases (those where remainder > SEGMENT_SIZE/4)
    //  * The 'end' handler checks if part === totalParts to decide finalization
    //  * When totalParts overestimated, part !== totalParts at EOF
    //  * Handler goes to else branch: increments part, calls callback(false) with
    //    empty this.data array
    //  * The buffered remainder (sitting in stream's internal buffer) is never
    //    consumed and is lost
    //
    // This test reproduces that scenario: 1 full segment + 1 remainder, where we
    // don't call read() after the first segment (simulating backpressure), causing
    // EOF to fire while the 32KB remainder sits unread in the stream buffer. The
    // 'end' handler then finalizes prematurely, and that buffered data is lost.

    const SEGMENT_SIZE = 128 * 1024;
    const FULLS = 1;
    const REM = 32 * 1024;
    const { filePath, original } = makeTestFile({
      fullSegments: FULLS, segSize: SEGMENT_SIZE, remainder: REM
    });

    const receivedParts = [];
    let completes = 0;

    const fu = new FileUtil({
      filePath,
      callback: ({ data, complete }) => {
        receivedParts.push(data);
        if (complete) completes += 1;
      }
    });

    fu.SEGMENT_SIZE = SEGMENT_SIZE;
    await fu.init();

    // Give stream time to buffer some data
    await new Promise(resolve => setTimeout(resolve, 100));

    fu.read();

    // Wait for the underlying stream to finish reading and fire 'end'
    // while the consumer hasn't called read() again due to backpressure
    await new Promise((resolve) => {
      setTimeout(() => {
        // After backpressure delay, consumer calls read() again to get remainder
        fu.read();
        // Give a moment for the callback to fire
        setTimeout(resolve, 50);
      }, 500);
    });

    const concatenated = concatUint8(receivedParts);

    // Should receive all data including what was buffered when EOF fired
    expect(concatenated.length).toBe(original.length);
    expect(completes).toBe(1);

    // Cleanup
    fs.unlinkSync(filePath);
  });
});
