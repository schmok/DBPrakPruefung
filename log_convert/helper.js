var path = require("path");
var fs = require("fs");
var readline = require('readline');

// HELPER FUNCTIONS
resolveTablePath = (tableName, json) => {
    const type = (json===true?"json":"csv");
    return path.resolve(__dirname, "../data/"+type+"/"+tableName.toUpperCase()+"_DATA_TABLE."+type);
};

class ProgressBar {
    constructor(printer, key, name) {
        this.printer = printer;
        this.key = key;
        this.type = "progress";
        this.name = name;
        this.content = this.name+": ";
        this.progress = 0;
    }

    setProgress(newProgress) {
        this.progress = newProgress;
        this.printer.updateConsole(this.key);
    };
}

class ConsoleText {
    constructor(printer, key, text) {
        this.printer = printer;
        this.key = key;
        this.type = "text";
        this.content = text;
    }

    setText(newText) {
        this.content = newText;
        this.printer.updateConsole(this.key);
    }
}

class ConsolePrinter {
    constructor() {
        this.rows = {};
        this.consoleWidth = 110;
    }

    updateConsole(rowKey) {
        var scope = this;
        var objectKeys = Object.getOwnPropertyNames(this.rows);

        let updateRow = (key, i) => {
            readline.cursorTo(process.stdout, 0, i);
            readline.clearLine(process.stdout);
            var row = scope.rows[key];
            var msg = "";
            switch(row.type) {
                case "text":
                    msg = row.content;
                    break;
                case "progress":
                    var percent = " " +(row.progress*100).toFixed(2)+" %";
                    var len = scope.consoleWidth - row.content.length - percent.length;
                    var progress = row.progress * len;
                    msg = row.content + ((new Array(0 | len)).join(" ").split(" ").map((s, i) => progress<i?" ":"░").join("")) + percent;
                    break;
            }
            process.stdout.write(msg+"\n");
        };
        if(rowKey === undefined) {
            readline.cursorTo(process.stdout, 0, 0);
            objectKeys.forEach((key, i) => {
                updateRow(key, i);
            });
        } else {
            updateRow(rowKey, objectKeys.indexOf(rowKey));
        }
    }

    newLine(id, text) {
        this.rows[id] = new ConsoleText(this, id, text);
        if(text !== undefined && text.trim.length > 0)
            this.updateConsole();
        return this.rows[id];
    }

    createProgressBar(key, name) {
        this.rows[key] = new ProgressBar(this, key, name);
        return this.rows[key];
    }
}

//
var bufferSize = 2 << 22;

let getCSVPath = (tableName) => resolveTablePath(tableName, false);
let getJSONPath = (tableName) => path.resolve(__dirname, "../data/json/"+tableName+".json");
let getKeyLookup = () => JSON.parse(fs.readFileSync(path.resolve(__dirname, "../data/key_lookup.json"), "utf8"));
let consolePrinter = new ConsolePrinter();

createLineStream = (filePath, batchSize, callback) => {
    var fd = fs.openSync(filePath, "r");
    var buffer = Buffer.alloc(bufferSize);
    var stats = fs.fstatSync(fd);
    var end = stats.size;
    var total = 0;
    var remainStr = "";
    var batch = [];

    while(total != end) {
        var bytesToRead = bufferSize < end-total?bufferSize:end-total;
        var read = fs.readSync(fd, buffer, 0, bytesToRead, total);
        total += read;
        var tmp = buffer.toString("utf8").substr(0, read);

        var lastBraceIndex = tmp.lastIndexOf("\n");
        var lines = (remainStr+tmp.substr(0, lastBraceIndex)).split("\n");

        if(batchSize > 0) {
            while(lines.length > 0) {
                if(batch.length < batchSize) {
                    batch = batch.concat(lines.splice(0, batchSize - batch.length));
                }

                if(batch.length === batchSize) {
                    callback(batch, total / end);
                    batch = [];
                }
            }
        } else {
            lines.forEach(line => {
                if(line.trim().length > 0)
                    callback(line, total / end);
            });
        }

        remainStr = tmp.substr(lastBraceIndex, tmp.length);
    }

    if(batch.length > 0) {
        callback(batch, total / end);
    }

    fs.closeSync(fd);
};

module.exports = {
    getCSVPath,
    getJSONPath,
    getKeyLookup,
    consolePrinter,
    createLineStream
};
