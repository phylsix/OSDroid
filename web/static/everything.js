$(document).ready(function() {
  var table = $("#wftable").DataTable({
    bProcessing: true,
    bServerSide: true,
    sPaginationType: "full_numbers",
    bjQueryUI: true,
    sAjaxSource: "/tables/everything_table",
    columns: [
      {
        className: "details-control",
        orderable: false,
        data: null,
        defaultContent: ""
      },
      { data: "Name" },
      { data: "Good" },
      { data: "ACDC" },
      { data: "Resubmit" },
      { data: "Timestamp" }
    ],
    order: [[5, "desc"]]
  });

  // Add event listener for opening and closing details
  $("#wftable tbody").on("click", "td.details-control", function() {
    var tr = $(this).closest("tr");
    var row = table.row(tr);

    if (row.child.isShown()) {
      // this row is already open - close it
      row.child.hide();
      tr.removeClass("shown");
    } else {
      // open this row
      row.child(format(row.data())).show();

      // ajax to get workflow history
      var name = row.data()["Name"];
      $.ajax({
        url: "/predhistory/" + name,
        success: function(rawdata) {
          var data = [
            {
              type: "scatter",
              mode: "lines",
              name: "Good",
              x: rawdata.map(x => x["timestamp"]),
              y: rawdata.map(x => x["good"]),
              line: { color: "#17BECF" }
            },
            {
              type: "scatter",
              mode: "lines",
              name: "Site Issue",
              x: rawdata.map(x => x["timestamp"]),
              y: rawdata.map(x => x["acdc"]),
              line: { color: "#7F7F7F" }
            },
            {
              type: "scatter",
              mode: "lines",
              name: "Workflow Issue",
              x: rawdata.map(x => x["timestamp"]),
              y: rawdata.map(x => x["resubmit"]),
              line: { color: "#d62728" }
            }
          ];

          var layout = { title: name };

          Plotly.newPlot("row_" + row.data()["id"], data, layout);

          tr.addClass("shown");
        }
      });
    }
  });
});
