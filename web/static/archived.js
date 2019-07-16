$(document).ready(function() {
  var table = $("#wftable").DataTable({
    bProcessing: true,
    bServerSide: true,
    sPaginationType: "full_numbers",
    bjQueryUI: true,
    sAjaxSource: "/tables/archived_table",
    columns: [
      {
        className: "details-control",
        orderable: false,
        data: null,
        defaultContent: ""
      },
      { data: "Label" },
      { data: "Name" },
      { data: "Good" },
      { data: "ACDC" },
      { data: "Resubmit" },
      { data: "Timestamp" }
    ],
    order: [[6, "desc"]], // asc

    rowCallback: function(row, data, index) {
      // actual labels
      if (data.Label == 0) {
        $(row)
          .find("td:eq(1)")
          .text("Good");
        $(row)
          .find("td:eq(2)")
          .css("color", "#17BECF");
        $(row)
          .find("td:eq(1)")
          .css("color", "#17BECF");
      } else if (data.Label == 1) {
        $(row)
          .find("td:eq(1)")
          .text("ACDC-ed");
        $(row)
          .find("td:eq(2)")
          .css("color", "#7F7F7F");
        $(row)
          .find("td:eq(1)")
          .css("color", "#7F7F7F");
      } else if (data.Label == 2) {
        $(row)
          .find("td:eq(1)")
          .text("Resubmitted");
        $(row)
          .find("td:eq(2)")
          .css("color", "#d62728");
        $(row)
          .find("td:eq(1)")
          .css("color", "#d62728");
      } else {
        $(row)
          .find("td:eq(1)")
          .text("Unknown");
      }

      // predictions
      if (data.Good > data.ACDC && data.Good > data.Resubmit) {
        $("td:eq(3)", row).css("color", "#17BECF");
      }
      if (data.ACDC > data.Good && data.ACDC > data.Resubmit) {
        $("td:eq(4)", row).css("color", "#7F7F7F");
      }
      if (data.Resubmit > data.Good && data.Resubmit > data.ACDC) {
        $("td:eq(5)", row).css("color", "#d62728");
      }
    }
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

      var rawdata = row.data()["History"];
      var trace_good = {
        type: "scatter",
        mode: "lines",
        name: "Good",
        x: rawdata.map(x => x["timestamp"]),
        y: rawdata.map(x => x["prediction"][0]),
        line: { color: "#17BECF" }
      };
      var trace_acdc = {
        type: "scatter",
        mode: "lines",
        name: "ACDC",
        x: rawdata.map(x => x["timestamp"]),
        y: rawdata.map(x => x["prediction"][1]),
        line: { color: "#7F7F7F" }
      };
      var trace_resub = {
        type: "scatter",
        mode: "lines",
        name: "Resubmit",
        x: rawdata.map(x => x["timestamp"]),
        y: rawdata.map(x => x["prediction"][2]),
        line: { color: "#d62728" }
      };

      var data = [trace_good, trace_acdc, trace_resub];
      var layout = { title: row.data()["Name"] };

      Plotly.newPlot("row_" + row.data()["id"], data, layout);

      tr.addClass("shown");
    }
  });
});
