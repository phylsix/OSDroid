$(document).ready(function() {
  $("#siteerrorTable").DataTable({
    ajax: { url: "/docs/site_errors", dataSrc: "data" },
    columns: [{ data: "site", width: "50%" }, { data: "errors", width: "20%" }],
    order: [[0, "asc"]],
    paging: false,
    drawCallback: function() {
      $("#siteerrorTable tbody tr td").css("padding", "2px 10px 2px 10px");
    }
  });
});
