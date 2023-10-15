latex_escape <- function (text) {
   latex <- text %>%
      str_replace_all("\\\\", "\\\\textbackslash") %>%
      str_replace_all("#", "\\\\#") %>%
      str_replace_all("\\$", "\\\\textdollar") %>%
      str_replace_all("%", "\\\\%") %>%
      str_replace_all("&", "\\\\&") %>%
      str_replace_all("\\^", "\\\\textcircumflex") %>%
      str_replace_all("_", "\\\\textunderscore") %>%
      str_replace_all("\\{", "\\\\textbraceleft") %>%
      str_replace_all("\\|", "\\\\textbar") %>%
      str_replace_all("\\}", "\\\\textbraceright") %>%
      str_replace_all("~", "\\\\textasciitilde")

   return(latex)
}










