var helper = require("./helper.js");
var printer = helper.consolePrinter;
var createLineStream = helper.createLineStream;
var fs = require("fs");
var path = require("path");

var name = "access_log";

var filePath = path.resolve(__dirname, "../logs/"+name);
var regex = /^(\S+) (\S+) (\S+) \[([^:]+):(\d+:\d+:\d+) ([^\]]+)\] "(\S+) (.+?) (\S+)" (\S+) (\S+)$/gm;
var fileProgress = printer.createProgressBar(name+"progress", "Reading "+name);
var fd = fs.openSync(path.resolve(__dirname, "../logs/"+name+".csv"), "w");
createLineStream(filePath, 1000, (lines, progress) => {
    lines.forEach(l => {
        if(regex.test(l)) {
            let c = l.split(regex).slice(1, 11);
            c = c.slice(0,3).concat([new Date(c.slice(3, 6).join(" ")).getTime()]).concat(c.slice(6,10));
            let buffer = c.map(s => `"${s}"`).join(",")+"\n";
            fs.writeSync(fd, buffer);
        }
    });

    fileProgress.setProgress(progress);
    if(progress === 1) {
        //fs.closeSync(fd);
    }
});
