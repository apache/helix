$(document).ready(function() {

    // Don't submit when enter is pressed anywhere on the page
    $(window).keydown(function(event){
        if(event.keyCode == 13) {
          event.preventDefault()
          return false
        }
    })

    $('#filter-add').click(function(event) {
        event.preventDefault()

        var div = $('<div></div>', {
            class: 'uk-form-row'
        })

        var select = $('<select></select>')
            .append($('<option></option>', {
                text: 'Partition',
                value: 0
            }))
            .append($('<option></option>', {
                text: 'Instance',
                value: 1
            }))
            .append($('<option></option>', {
                text: 'Ideal',
                value: 2
            }))
            .append($('<option></option>', {
                text: 'External',
                value: 3
            }))

        var input = $('<input></input>', {
            type: 'text'
        }).keyup(function() {
            filterTable("#filter-form", "#resource-state-table")
        })

        var closeButton = $('<a></a>', {
            class: 'uk-close'
        }).click(function() {
            div.remove()
            filterTable("#filter-form", "#resource-state-table")
        })

        div.append(select)
           .append(input)
           .append(closeButton)

        $("#filter-form").append(div)
    })
})