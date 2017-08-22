var helper = require("./helper.js");
var printer = helper.consolePrinter;
var createLineStream = helper.createLineStream;
var fs = require("fs");
var path = require("path");

var name = "is-access_log";

var filePath = path.resolve(__dirname, "../logs/"+name);
var regex = /^(\S+) (\S+) (\S+) \[([^:]+):(\d+:\d+:\d+) ([^\]]+)\] "(\S+ )?(.+?)?( \S+)?" (\S+) (\S+)$/;
var fileProgress = printer.createProgressBar(name+"progress", "Reading "+name);
var fd = fs.openSync(path.resolve(__dirname, "../logs/"+name+".csv"), "w");
var cnt = 0;
createLineStream(filePath, 1000, (lines, progress) => {
    lines.some(l => {
        if(regex.test(l)) {
            let c = l.split(regex).slice(1, 12);
            c = c.slice(0,3).concat([new Date(c.slice(3, 6).join(" ")).getTime()+""]).concat(c.slice(6,11));
            let buffer = c.map(s => s!=="-"&&s!==undefined?`"${s.trim()}"`:"").join(",")+"\n";
            fs.writeSync(fd, buffer);
            cnt++;
            if(cnt  > 10) {
                return true;
            }
        } else {
            var line = l.split(regex);
            var t = regex.test(l);
            var c = 123;
        }
    });

    if(cnt  > 10) {
        return true;
    }

    fileProgress.setProgress(1-progress);
    if(progress === 1) {
        //fs.closeSync(fd);
    }
});
