pacman::p_load(
   dplyr,
   haven,
   shiny,
   shinythemes,
   DT
)

pii <- read_rds(Sys.getenv("DEDUP_PII"))
pii %<>% filter(is.na(DELETED_AT)) %>%
   select(-REC_ID, -SNAPSHOT, -DELETED_AT) %>%
   distinct_all()

shinyApp(
   ui      = fluidPage(
      theme = shinythemes::shinytheme('flatly'),
      DTOutput('tbl'),
      tags$script(
         HTML("
jQuery('#tbl').on('draw.dt', function() {
   jQuery('tbody tr td').on('click', function () {
        var temp = jQuery('<input>');
        jQuery('body').append(temp);
        temp.val(jQuery(this).clone().children().remove().end().text().trim()).select();
        document.execCommand('copy');
        temp.remove();
    })
});
")
      ),
      htmltools::htmlDependency(
         "jquery",
         "3.3.1",
         src    = c(href = "https://code.jquery.com/"),
         script = "jquery-3.3.1.min.js"
      ),
   ),
   server  = function(input, output) {
      observeEvent(input$browser, {
         browser()
      })

      output$tbl <- DT::renderDT(
         datatable(
            pii %>% mutate_all(~as.character(.)),
            filter    = 'top',
            rownames  = F,
            selection = list(mode = 'single', target = 'cell'),
            options   = list(
               pageLength = 100,
               lengthMenu = c(100, 150, 200),
               order      = list(list(2, 'asc'), list(4, 'desc'))
            ),
            style     = 'bootstrap'
         )
      )

   },
   options = list(host           = '0.0.0.0',
                  port           = 6984,
                  launch.browser = F)
)
