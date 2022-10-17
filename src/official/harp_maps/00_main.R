p_load(
   geojsonio,
   broom,
   ggplot2,
   viridis,
   showtext,
   rgdal,
   sf,
   svglite
)

font_add(
   "Infra",
   regular    = "H:/_Resources/Fonts/Infra/InfraRegular.otf",
   bold       = "H:/_Resources/Fonts/Infra/InfraBold.otf",
   italic     = "H:/_Resources/Fonts/Infra/InfraRegularItalic.otf",
   bolditalic = "H:/_Resources/Fonts/Infra/InfraBoldItalic.otf",
)
showtext_auto()

sizeit <- function(p, panel.size = 2, default.ar = 1) {

   gb <- ggplot_build(p)
   # first check if theme sets an aspect ratio
   ar <- gb$plot$coordinates$ratio

   # second possibility: aspect ratio is set by the coordinates, which results in
   # the use of 'null' units for the gtable layout. let's find out
   g     <- ggplot_gtable(gb)
   nullw <- sapply(g$widths, attr, "unit")
   nullh <- sapply(g$heights, attr, "unit")

   # ugly hack to extract the aspect ratio from these weird units
   if (any(nullw == "null"))
      ar <- unlist(g$widths[nullw == "null"]) / unlist(g$heights[nullh == "null"])

   if (is.null(ar)) # if the aspect ratio wasn't specified by the plot
      ar <- default.ar

   # ensure that panel.size is always the larger dimension
   if (ar <= 1) panel.size <- panel.size / ar

   g$fullwidth  <- grid::convertWidth(sum(g$widths), "in", valueOnly = TRUE) +
      panel.size
   g$fullheight <- grid::convertHeight(sum(g$heights), "in", valueOnly = TRUE) +
      panel.size / ar

   class(g) <- c("sizedgrob", class(g))
   g
}


print.sizedgrob <- function(x) {
   # note: dev.new doesn't seem to respect those parameters
   # when called from Rstudio; in this case it
   # may be replaced by x11 or quartz or ...
   dev.new(width = x$fullwidth, height = x$fullheight)
   grid::grid.draw(x)
}


ggGetAr <- function(p, default.ar = -1) {

   gb <- ggplot_build(p)
   # first check if theme sets an aspect ratio
   ar <- gb$plot$coordinates$ratio

   # second possibility: aspect ratio is set by the coordinates, which results in
   # the use of 'null' units for the gtable layout. let's find out
   g     <- ggplot_gtable(gb)
   nullw <- sapply(g$widths, attr, "unit")
   nullh <- sapply(g$heights, attr, "unit")

   # ugly hack to extract the aspect ratio from these weird units
   if (any(nullw == "null"))
      ar <- unlist(g$widths[nullw == "null"]) / unlist(g$heights[nullh == "null"])

   if (is.null(ar)) # if the aspect ratio wasn't specified by the plot
      ar <- default.ar

   ar[1]
}
