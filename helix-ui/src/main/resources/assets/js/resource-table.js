$(document).ready(function() {

    // Don't submit when enter is pressed anywhere on the page
    $(window).keydown(function(event){
        if(event.keyCode == 13) {
          event.preventDefault()
          return false
        }
    })

    $('#resource-filter-add').click(function(event) {
        event.preventDefault()

        var div = $('<div></div>', {
            class: 'uk-form-row'
        })

        var select = $('<select></select>')
            .append($('<option></option>', {
                text: 'Name',
                value: 0
            }))

        var input = $('<input></input>', {
            type: 'text'
        }).keyup(function() {
            filterTable("#resource-filter-form", "#resource-table")
        })

        var closeButton = $('<a></a>', {
            class: 'uk-close'
        }).click(function() {
            div.remove()
            filterTable("#resource-filter-form", "#resource-table")
        })

        div.append(select)
           .append(input)
           .append(closeButton)

        $("#resource-filter-form").append(div)
    })
})
