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

    // first create document
    //createDocument("access_log", db);
    createDocument("is-access_log", db);
});

function createDocument(documentName, db) {
    db.collection("servers").updateOne( { server_name: documentName}, { $set:{server_name: documentName} }, {upsert:true}, () => {
        convertAndWriteLog(documentName, false, db);
    });
}

function convertAndWriteLog(name, writeCSV, db) {
    var filePath = path.resolve(__dirname, "../logs/"+name);
    var regex = /^(\S+) (\S+) (\S+) \[([^:]+):(\d+:\d+:\d+) ([^\]]+)\] "(\S+ )?(.+?)?( \S+)?" (\S+) (\S+)(?: "([^"]*)")?(?: "([^"]*)")?$/;
    var fileProgress = printer.createProgressBar(name+"progress", "Reading "+name);
    var fd = fs.openSync(path.resolve(__dirname, "../logs/"+name+".csv"), "w");
    var lastDate;
    var abbort = false;
    var dateKeys = {};

    createLineStream(filePath, 1000, (lines, progress) => {
        var bulk = db.collection('servers_data').initializeUnorderedBulkOp();
        lines.some(l => {
            if(regex.test(l)) {
                let c = l.split(regex).slice(1, 14);
                let d = new Date(c.slice(3, 6).join(" "));
                if(lastDate === undefined) {
                    lastDate = new Date(d);
                    lastDate.setFullYear(lastDate.getFullYear() -1);
                }
                
                if(lastDate <= d) {
                    c = c.map(s => s!=="-"&&s!==undefined?s.trim():"");
                    var data = {
                                "hostname": c[0],
                                "remote_logname": c[1],
                                "user_id": c[2],
                                "request_time": mongo.Long.fromNumber(d.getTime()),
                                "method": c[6],
                                "url": c[7],
                                "protocol": c[8],
                                "status_code": parseInt(c[9]),
                                "response_body_size": parseInt(c[10]),
                                "referrer": c[11],
                                "user_agent": c[12]
                            };
                    // check if month is present in objects;
                    var dateKey = [d.getYear()+1900, d.getMonth(), d.getDate()].join(".");
                    if(dateKeys[dateKey] === undefined) {
                        var id = new ObjectID();
                        db.collection("servers").updateOne({ server_name: name }, 
                        { 
                            $set: {
                                [`accesses.${dateKey}.data`]: {
                                    $ref: "servers_data",
                                    $id: id,
                                    $db: "pruefung"
                                }
                            } 
                        });
                        dateKeys[dateKey] = id;
                    }

                    
                    bulk.find( { _id: dateKeys[dateKey] } ).upsert().updateOne({
                        $set: { server_name: name, date: dateKey },
                        $push: {
                            [`accesses`] : 
                                Object.keys(data)
                                    .filter((k) => data[k] !== undefined)
                                    .reduce((p, c) => {
                                        p[c] = data[c];
                                        return p;
                                    }, {})
                        }
                    });
                    

                    if(writeCSV) {
                        c = c.slice(0,3).concat([d.getTime()+""]).concat(c.slice(6,13));
                        let buffer = c.map((s) => `"${s.replace(/"/g, "\\\"")}"`).join(",")+`,"${name==="access-log"?2:1}"\n`;
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


