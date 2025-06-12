#' Run the VibeCheck Shiny application
#'
#' @param authenticate Whether to enable authentication using shinymanager
#' @param debug Whether to enable debug mode with additional logging
#'
#' @return A Shiny application object
#' @export
#'
#' @examples
#' \dontrun{
#' VibeCheck()
#' VibeCheck(authenticate = FALSE)
#' VibeCheck(debug = TRUE)
#' }
VibeCheck <- function(authenticate = TRUE, debug = FALSE) {
  # Set debug option
  options(vibe_check.debug = debug)

  # Source global, UI, and server code from the inst/app directory
  source(system.file("app", "global.R", package = "VibeCheck"), local = TRUE)
  source(system.file("app", "app_ui.R", package = "VibeCheck"), local = TRUE)
  source(
    system.file("app", "app_server.R", package = "VibeCheck"),
    local = TRUE
  )

  # Optionally wrap the UI with shinymanager if authentication is enabled
  if (authenticate) {
    if (!requireNamespace("shinymanager", quietly = TRUE)) {
      stop(
        "Package 'shinymanager' is needed for authentication. Please install it.",
        call. = FALSE
      )
    }
    app_ui <- shinymanager::secure_app(
      app_ui,
      enable_admin = TRUE,
      fab_position = "bottom-left"
    )
  }

  # Launch the Shiny application
  shiny::shinyApp(
    ui = app_ui,
    server = server,
    options = list(launch.browser = TRUE)
  )
}
