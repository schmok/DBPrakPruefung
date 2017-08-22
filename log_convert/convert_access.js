var helper = require("./helper.js");
var printer = helper.consolePrinter;
var createLineStream = helper.createLineStream;
var fs = require("fs");
var path = require("path");

var name = "access_log";

var filePath = path.resolve(__dirname, "../logs/"+name);
var regex = /^(\S+) (\S+) (\S+) \[([^:]+):(\d+:\d+:\d+) ([^\]]+)\] "(\S+ )?(.+?)?( \S+)?" (\S+) (\S+)(?: "([^"]*)")?(?: "([^"]*)")?$/;
var fileProgress = printer.createProgressBar(name+"progress", "Reading "+name);
var fd = fs.openSync(path.resolve(__dirname, "../logs/"+name+".csv"), "w");
var lastDate;
var abbort = false;
createLineStream(filePath, 1000, (lines, progress) => {
    lines.some(l => {
        if(regex.test(l)) {
            let c = l.split(regex).slice(1, 14);
            let d = new Date(c.slice(3, 6).join(" "));
            if(lastDate === undefined) {
                lastDate = new Date(d);
                lastDate.setFullYear(lastDate.getFullYear() -1);
            }

            if(lastDate >= d) {
                c = c.slice(0,3).concat([d.getTime()+""]).concat(c.slice(6,13));
                let buffer = c.map(s => s!=="-"&&s!==undefined?`"${s.trim()}"`:"").join(",")+`,"${name}"\n`;
                fs.writeSync(fd, buffer);
            }
        } else {
            // errors here 
        }
    });
    fileProgress.setProgress(1-progress);
    //return abbort;
});
