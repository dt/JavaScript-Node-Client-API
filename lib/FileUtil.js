const fs = require('fs');
const path = require("path");

function FileUtil(param) {

    if(param === undefined || typeof param !== 'object') {
        throw new Error('FileUtil: Invalid parameters');
    }

    if(!param.hasOwnProperty('filePath')) {
        throw new Error('FileUtil: filePath is needed');
    }

    if(!param.hasOwnProperty('callback')) {
        throw new Error('FileUtil: callback is needed');
    }

    if(param.hasOwnProperty('callback') && typeof param.callback !== 'function') {
        throw new Error('FileUtil: callback must be a function');
    }

    let myself = this;
    this.SEGMENT_SIZE = 2621440;
    this.filePath = param.filePath;
    this.callback = param.callback;
    this.eof = false;
    this.data = [];
    this.tempSize = 0;

    this.init = function() {
        return new Promise(function(resolve) {
            fs.stat(myself.filePath, (err, stats) => {
                if (err) {
                    throw new Error('FileUtil: File does not exist, ' + myself.filePath);
                } else {
                    resolve({
                        name: path.basename(myself.filePath),
                        size: stats.size,
                        // NB: totalParts is a simple N/k, unlike other implementations which have a
                        // special-case for a smaller, 1/4 size first part.
                        totalParts: Math.ceil(stats.size / myself.SEGMENT_SIZE),
                    });

                    myself.stream = fs.createReadStream(myself.filePath);

                    myself.stream.on('readable', function () {
                        // If a `read` call marked us as waiting, go ahead and
                        // process-and-maybe emit the readable data. Doing this
                        // only if a read call has marked us as waiting ensures
                        // we don't just keep reading and processing as data
                        // is available ahead of the consumer's consumption.
                       if (myself.waitingForRead) {
                            process();
                        }
                    });

                    myself.stream.on('end', function () {
                        // Set EOF so read can determine it is finished once it
                        // reads completely from the stream, then, if we are
                        // waiting since a call to `read`, go ahead and process
                        // now.
                        myself.eof = true;
                        if (myself.waitingForRead) {
                            process();
                        }
                    });
                }
            });
        });
    }

    this.read = function() {
        myself.waitingForRead = true;
        process();
    }

    function process() {
        // Drain whatever is available in the stream buffer to this.data before
        // determining if we have a full segment and/or if we are done. MB: it's
        // vital we fully drain the stream buffer after an EOF before we can
        // declare completion -- fully draining it every call is a simple way
        // to ensure that.
        let chunk;
        while ((chunk = myself.stream.read()) !== null) {
            myself.data.push(new Uint8Array(chunk));
            myself.tempSize += chunk.length;
        }

        // If we have a full segment or more buffered, or if we are at EOF (and
        // have fully drained the stream buffer above), we can emit what we have
        // or an empty final segment to signal completion.
        if (myself.tempSize >= myself.SEGMENT_SIZE || myself.eof) {
            if (myself.tempSize > 0) {
                const buf = concatenate(myself.data);
                const seg = buf.slice(0, myself.SEGMENT_SIZE);
                const remainder = buf.slice(myself.SEGMENT_SIZE);
                myself.data = remainder.length > 0 ? [remainder] : [];
                myself.tempSize = remainder.length;
                myself.callback({data: seg, complete: myself.eof && myself.tempSize === 0});
            } else {
                // If we got here, it means there was no data in the buffer after
                // draining the stream after EOF, so we're done and just need to
                // emit an empty final segment to signal completion.
                myself.callback({data: new Uint8Array(0), complete: true});
            }
            myself.waitingForRead = false;
        }
    }

    function concatenate(arrays) {
        var totalLength = arrays.reduce(function(total, arr) {
            return total + arr.length
        }, 0);
        var result = new Uint8Array(totalLength);
        arrays.reduce(function(offset, arr){
            result.set(arr, offset);
            return offset + arr.length;
        }, 0);
        return result;
    }
}

module.exports = {FileUtil};