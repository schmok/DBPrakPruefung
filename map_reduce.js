// all errors
db.servers.mapReduce(
    function() { 
       Object.keys(this.errors).forEach(y => {
            Object.keys(this.errors[y]).forEach(m => {
                Object.keys(this.errors[y][m]).forEach(d => {
                    emit("total", this.errors[y][m][d].length);
                });
            });
       });
    },
    function(key, values) { return Array.sum(values); },
    {
        query: {  },
        out: "all_errors"
})

