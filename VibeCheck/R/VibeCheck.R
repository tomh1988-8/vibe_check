VibeCheck <- function(authenticate = TRUE, debug = FALSE) {
  # Source global, UI, and server code from the inst/app directory
  source("inst/app/global.R", local = TRUE)
  source("inst/app/app_ui.R", local = TRUE)
  source("inst/app/app_server.R", local = TRUE)

  # Ensure that the UI is assigned to the expected variable.
  # If your UI is defined as 'app_ui' in app_ui.R, assign it to 'ui' here:
  ui <- app_ui

  # Optionally wrap the UI with shinymanager if authentication is enabled
  if (authenticate) {
    ui <- shinymanager::secure_app(
      ui,
      enable_admin = TRUE,
      fab_position = "bottom-left"
    )
  }

  # Launch the Shiny application
  shiny::shinyApp(
    ui = ui,
    server = server,
    option = list(launch.browser = TRUE)
  )
}
