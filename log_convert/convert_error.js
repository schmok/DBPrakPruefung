var helper = require("./helper.js");
var printer = helper.consolePrinter;
var createLineStream = helper.createLineStream;
var fs = require("fs");
var path = require("path");
var mongo = require("mongodb");
var ObjectID = mongo.ObjectID;
var client = mongo.MongoClient;

var url = "mongodb://localhost/pruefung";

client.connect(url, (err, db) => {
    if(err) throw err;

    convertAndWriteLog("error_log", "access_log", false, db);
    //convertAndWriteLog("is-error_log", "is-access_log", false, db);
});

function convertAndWriteLog(name, target, writeCSV, db) {
    var filePath = path.resolve(__dirname, "../logs/"+name);
    var regex = /^\[([^\]]+)\] \[([^\]]+)\] (?:\[([^\]]+)\] )?(.*)$/;
    var fileProgress = printer.createProgressBar(name+"progress", "Reading "+name);
    var fd = fs.openSync(path.resolve(__dirname, "../logs/"+name+".csv"), "w");
    var lastDate;
    var abbort = false;
    createLineStream(filePath, 1000, (lines, progress) => {
        var bulk = db.collection('servers').initializeUnorderedBulkOp();
        lines.some(l => {
            if(regex.test(l)) {
                let c = l.split(regex).slice(1, 5);
                let d = new Date(c[0]);
                if(lastDate === undefined) {
                    lastDate = new Date(d);
                    lastDate.setFullYear(lastDate.getFullYear() -1);
                }

                if(lastDate <= d) {
                    c = [d.getTime()+""].concat(c.slice(1,3)).concat(c[3].split(": "));
                    if(c[2] === undefined || c[2].indexOf("client") === -1) {
                        if(c[2] === undefined) {

                        } else
                            console.log(c[2]);
                    } else {
                        c[2] = c[2].replace("client ", "");
                    }

                    c = c.map(s => s!=="-"&&s!==undefined?s.trim():undefined);
                    
                    var data = {
                                "error_time": mongo.Long.fromNumber(d.getTime()),
                                "type": c[1],
                                "hostname": c[2],
                                "cause": c[3],
                                "message": c[4]
                    };
                    bulk.find( { server_name: target } ).upsert().updateOne({
                        $push: {
                            [`errors.${[d.getYear()+1900, d.getMonth(), d.getDay()].join(".")}`] : 
                                Object.keys(data)
                                    .filter((k) => data[k] !== undefined)
                                    .reduce((p, c) => {
                                        p[c] = data[c];
                                        return p;
                                    }, {})
                        }
                    });

                    if(writeCSV) {
                        let buffer = c.map(s =>`"${s.trim()}"`).join(",")+`,"${name}"\n`;
                        fs.writeSync(fd, buffer);
                    }
                } else {
                    abbort = true;
                    return true;
                }
            } else {
                // errors here 
            }
        });
        bulk.execute(() => {
            
        });

        fileProgress.setProgress(1-progress);
        return abbort;
    });
}