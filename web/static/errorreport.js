function timeConverter(UNIX_timestamp) {
  var a = new Date(UNIX_timestamp * 1000);
  return a.toUTCString();
}

$(document).ready(function() {
  var wfid = $("#wfId").html(); // get workflow name
  $.getJSON("/docs/errorreport/" + wfid, function(rawdata) {
    $("#_summaryTable").DataTable({
      data: [
        [
          rawdata["type"],
          rawdata["status"],
          rawdata["totalError"],
          rawdata["failureRate"].toFixed(6)
        ]
      ],
      columns: [
        { title: "Type", orderable: false },
        { title: "Status", orderable: false },
        { title: "TotalError", orderable: false },
        { title: "FailureRate", orderable: false }
      ],
      order: [],
      dom: "t"
    });

    $("#_failurekeywords").html(
      rawdata["failureKeywords"]
        .map(function(kw) {
          return "<span class='tags'>" + kw + "</span>";
        })
        .join(" ")
    );

    $("#_transitionTable").DataTable({
      data: rawdata["transitions"].map(row =>
        Object.assign(row, { UpdateTime: timeConverter(row["UpdateTime"]) })
      ),
      columns: [
        { data: "DN", orderable: false },
        { data: "Status", orderable: false },
        { data: "UpdateTime", orderable: true }
      ],
      order: [],
      dom: "t"
    });

    var taskData = [];
    rawdata["tasks"].forEach(function(task) {
      var task_t = {
        data: {
          Name: task["name"],
          JobType: task["jobType"],
          SiteErrors: task["siteErrors"]
            .map(se => se["site"] + ": " + se["counts"])
            .join("</br>"),
          SiteNotReported: task["siteNotReported"].join(", "),
          InputTask: task["inputTask"]
        },
        kids: []
      };
      task["errors"].forEach(function(err) {
        var err_t = {
          data: {
            Code: err["errorCode"],
            Count: err["counts"],
            Keywords: err["errorKeywords"].join(", "),
            SecondaryCodes: err["secondaryErrorCodes"].join(", "),
            Site: err["siteName"],
            Timestamp: timeConverter(err["timeStamp"])
          },
          kids: []
        };
        err["errorChain"].forEach(function(chain) {
          var chain_t = {
            data: {
              ExitCode: chain["exitCode"],
              ErrorType: chain["errorType"],
              Description: chain["description"]
            },
            kids: []
          };
          err_t["kids"].push(chain_t);
        });
        task_t["kids"].push(err_t);
      });
      taskData.push(task_t);
    });

    var taskNestedSettings = {
      iDisplayLength: 20,
      bLengthChange: false,
      paging: false,
      bFilter: false,
      bSort: false,
      bInfo: false
    };
    var taskTable = new nestedTables.TableHierarchy(
      "_taskTable",
      taskData,
      taskNestedSettings
    );
    taskTable.initializeTableHierarchy();
  }).fail(function() {
    alert("No error report for\n"+wfid+"\navailable!");
  });
});
