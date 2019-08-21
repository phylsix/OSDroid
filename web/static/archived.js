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
      {
        className: "correctness",
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
    order: [[7, "desc"]], // asc
    pageLength: 50,
    lengthMenu: [[50, 100, 150, 200, 250], [50, 100, 150, 200, 250]],

    rowCallback: function(row, data, index) {
      // actual labels
      if (data.Label == 0) {
        $(row)
          .find("td:eq(1)")
          .text("Good");
        $(row)
          .find("td:eq(3)")
          .css("color", "#17BECF");
        $(row)
          .find("td:eq(1)")
          .css("background", "#17BECF")
          .css("color", "white");
      } else if (data.Label == 1) {
        $(row)
          .find("td:eq(1)")
          .text("Site Issue");
        $(row)
          .find("td:eq(3)")
          .css("color", "#7F7F7F");
        $(row)
          .find("td:eq(1)")
          .css("background", "#7F7F7F")
          .css("color", "white");
      } else if (data.Label == 2) {
        $(row)
          .find("td:eq(1)")
          .text("Workflow Issue");
        $(row)
          .find("td:eq(3)")
          .css("color", "#d62728");
        $(row)
          .find("td:eq(1)")
          .css("background", "#d62728")
          .css("color", "white");
      } else {
        $(row)
          .find("td:eq(1)")
          .text("Unknown");
      }

      // predictions
      if (data.Good > data.ACDC && data.Good > data.Resubmit) {
        $("td:eq(4)", row).css("color", "#17BECF");
        if (data.Label == 0) {
          $("td:eq(2)", row).css("background", "#a1d99b");
        } else if (data.Label > 0) {
          $("td:eq(2)", row).css("background", "#d95f0e");
        }
      }
      if (data.ACDC > data.Good && data.ACDC > data.Resubmit) {
        $("td:eq(5)", row).css("color", "#7F7F7F");
        if (data.Label == 1) {
          $("td:eq(2)", row).css("background", "#a1d99b");
        } else if (data.Label >= 0) {
          $("td:eq(2)", row).css("background", "#d95f0e");
        }
      }
      if (data.Resubmit > data.Good && data.Resubmit > data.ACDC) {
        $("td:eq(6)", row).css("color", "#d62728");
        if (data.Label == 2) {
          $("td:eq(2)", row).css("background", "#a1d99b");
        } else if (data.Label >= 0) {
          $("td:eq(2)", row).css("background", "#d95f0e");
        }
      }

      $(row)
        .find("td:eq(3)")
        .html(
          data.Name +
            ' | <a href="https://cms-unified.web.cern.ch/cms-unified//report/' +
            data.Name +
            '" style="font-size: small;" target="_blank">unified</a>'
        );
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

  // explain text
  $("#tooltipbtn").click(function() {
    $("#bigTooltip").toggle("fast", function() {
      if ($(this).is(":visible")) {
        $("#tooltipbtn").text("hide explain text");
      } else {
        $("#tooltipbtn").text("show explain text");
      }
    });
  });
});
