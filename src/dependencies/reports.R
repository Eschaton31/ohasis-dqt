base64_img_uri <- function(file) {
   uri <- knitr::image_uri(file)
   uri <- sprintf("<img src=\"%s\" />\n", uri)
   return(uri)
}

template_replace <- function(template, key_val) {
   keys <- names(key_val)
   vals <- key_val

   pairs <- length(key_val)
   for (i in pairs) {
      template <- stri_replace_all_fixed(template, stri_c("{{", keys[[i]], "}}"), vals[[i]])
   }

   return(template)
}
