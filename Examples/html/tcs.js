(function() {
    // Create the connector object
    var tableauConnector = tableau.makeConnector();

    // Define the schema
    tableauConnector.getSchema = function(schemaCallback) {
        // Schema for Transport Capacity Statistics Table
        var tcs_cols = [{
            id: "dropsPerHour",
            alias: "Drops per Hour",
            dataType: tableau.dataTypeEnum.float
        }, {
            id: "bucketId",
            alias: "Bucket ID",
            dataType: tableau.dataTypeEnum.int
        }, {
            id: "plannedNumDrops",
            alias: "Planned Number of Drops",
            dataType: tableau.dataTypeEnum.int
        }, {
            id: "numVehicles",
            alias: "Number of Vehicles",
            dataType: tableau.dataTypeEnum.int
        }, {
            id: "shiftStatus",
            alias: "Shift Status",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "deliveryDate",
            alias: "Delivery Date",
            dataType: tableau.dataTypeEnum.date
        }, {
            id: "lastMileShift",
            alias: "Last Mile Shift",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "numOrders",
            alias: "Number of Orders",
            dataType: tableau.dataTypeEnum.int
        }, {
            id: "utilizedNumDrops",
            alias: "Utilized Number of Drops",
            dataType: tableau.dataTypeEnum.int
        }];

        var tcsTable = {
            id: "tcsTable",
            alias: "Transport Capacity Statistics Table",
            columns: tcs_cols
        };

        schemaCallback([tcsTable]);
    };

    // Download the data
    tableauConnector.getData = function(table, doneCallback) {
        var past = 2;
        var future = 7;
        var totalDays = past + future + 1;

        var timeout = setTimeout(function() { doneCallback(); }, 20000);

        for (dateDelta = -past; dateDelta <= future; dateDelta ++) {
            var date = new Date();
            date.setDate(date.getDate() + dateDelta);
            var dateString = new Date(date).toISOString().substr(0,10);
            var apiCall = "https://transport-capacity.warehouse-api.alpha.redmart.com/v1.0.0/v1/shiftMgmt/" + dateString;
            // var apiCall = "http://warehouse.internal-lb.service.redmart:9109/v1/shiftMgmt/" + dateString;
            
            $.ajax({
                beforeSend: function(request) {
                    request.setRequestHeader("Authorization", tableau.connectionData);
                },
                dataType: "json",
                url: apiCall,
                error: function (xhr, ajaxOptions, thrownError) {
                    tableau.log(xhr.responseText);
                },
                success: function(resp) {
                    var buckets = resp.buckets,
                        tableData = [];

                    var row = 0;

                    if (table.tableInfo.id == "tcsTable") {
                        for (row = 0, len = buckets.length; row < len; row++) {
                            tableData.push({
                                "dropsPerHour": Math.round(buckets[row].dropsPerHour * 100) / 100,
                                "bucketId": buckets[row].bucketId,
                                "plannedNumDrops": buckets[row].plannedCapacity.numDrops,
                                "numVehicles": buckets[row].numVehicles,
                                "shiftStatus": buckets[row].shiftStatus,
                                "deliveryDate": buckets[row].lastMileShiftReference.deliveryDate,
                                "lastMileShift": buckets[row].lastMileShiftReference.shift,
                                "numOrders": buckets[row].numOrders,
                                "utilizedNumDrops": buckets[row].utilizedCapacity.numDrops
                            });
                        }
                    }

                    table.appendRows(tableData);
                    totalDays --;

                    if (totalDays == 0) {
                        clearTimeout(timeout);
                        doneCallback();
                    }
                }
            });
        }
    };

    tableau.registerConnector(tableauConnector);

    // Create event listeners for when the user submits the form
    $(document).ready(function() {
        $("#submitButton").click(function() {
//            tableau.connectionData = JSON.stringify(dateObj); // Use this variable to pass data to your getSchema and getData functions
            tableau.connectionData = $('#token').val().trim();
            tableau.connectionName = "Transport Capacity Statistics"; // This will be the data source name in Tableau
            tableau.submit(); // This sends the connector object to Tableau
        });
    });
})();
