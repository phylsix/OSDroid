$(document).ready(function() {
  $('#issuesettingsBtn').click(function() {
    $('#issuesettings').toggle('fast', function() {
      if ($(this).is(':visible')) {
        $('#issuesettingsBtn').text('Hide settings');
      } else {
        $('#issuesettingsBtn').text('Make new settings');
      }
    });
  });
});