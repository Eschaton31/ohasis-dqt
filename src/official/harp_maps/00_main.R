p_load(
   geojsonio,
   broom,
   ggplot2,
   viridis,
   showtext,
   rgdal,
   sf,
   svglite,
   tmaptools,
   ragg,
   rmapshaper
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

plot_map <- function(data, diff_to = NULL, strokecolor = NA, fillcolor = c('#097FB3', '#A13675'),
                     alpha = 1, graticules = FALSE, zoom_to = NULL, zoom_level = NULL) {
  target_crs <- st_crs(data)

  if (!is.null(zoom_level)) {
    if (is.null(zoom_to)) {
      zoom_to_xy <- st_sfc(st_point(c(0, 0)), crs = target_crs)
    } else {
      zoom_to_xy <- st_transform(st_sfc(st_point(zoom_to), crs = 4326), crs = target_crs)
    }

    C <- 40075016.686   # ~ circumference of Earth in meters
    x_span <- C / 2^zoom_level
    y_span <- C / 2^(zoom_level+1)

    disp_window <- st_sfc(
        st_point(st_coordinates(zoom_to_xy - c(x_span / 2, y_span / 2))),
        st_point(st_coordinates(zoom_to_xy + c(x_span / 2, y_span / 2))),
        crs = target_crs
    )

    xlim <- st_coordinates(disp_window)[,'X']
    ylim <- st_coordinates(disp_window)[,'Y']
  } else {
    xlim <- NULL
    ylim <- NULL
  }

  if (!is.null(diff_to)) {
    shapebase <- diff_to
    shapediff <- st_sym_difference(data, diff_to)
  } else {
    shapebase <- data
    shapediff <- NULL
  }

  p <- ggplot() +
    geom_sf(data = shapebase, color = strokecolor, fill = fillcolor[1], alpha = alpha)

  if (!is.null(shapediff)) {
    p <- p + geom_sf(data = shapediff, color = strokecolor, fill = fillcolor[2], alpha = alpha)
  }

  p +
    coord_sf(xlim = xlim,
             ylim = ylim,
             crs = target_crs,
             datum = ifelse(graticules, target_crs$input, NA)) +
    theme_bw()
}