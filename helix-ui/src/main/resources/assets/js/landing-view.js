$(document).ready(function() {
    $("#landing-form-button").click(function(event) {
        event.preventDefault()
        window.location = "/dashboard/" + encodeURIComponent($("#zk-address").val())
    })
})