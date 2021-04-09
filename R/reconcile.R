build_query <- function(data, query_col, property_cols, type, limit) {

  payload <- select(data, query = query_col, property_cols)

  if(!missing(property_cols)) {
    payload$properties <-
      pmap(payload, function(...) {
        row <- list(...)
        map2(names(row[property_cols]), row[property_cols], function(pid, v) {
          list(pid = pid, v = v)
        })
      })
  }

  if(!missing(type)) {
    payload$type <- type
  }

  if(!missing(limit)) {
    payload$limit <- limit
  }

  payload <- payload %>%
    select(-property_cols) %>%
    transpose()

  names(payload) <-
    seq(0, length(payload) - 1) %>%
    str_c("q", .)

  payload
}

send_request <- function(payload, endpoint) {
  response <-
    POST(
      url = endpoint,
      body = list(queries = toJSON(payload, auto_unbox = TRUE)))

  response %>%
    content("text", encoding = "UTF-8") %>%
    fromJSON(simplifyVector = FALSE)
}

parse_results <- function(results) {
  results %>%
    map("result") %>%
    map_dfr(~tibble(data = .x), .id = "query_id") %>%
    unnest_wider(data)
}

#' Reconcile a data frame against an external data source
#'
#' This is the primary function for matching data against a reconciliation
#' endpoint using the \href{https://reconciliation-api.github.io/specs/latest/}{Reconciliation Service API standard}.
#'
#' @param data A data frame of candidates for reconciliation, one per row
#' @param endpoint The URL of the reconciliation API endpoint, including API key
#'   if required
#' @param query_col The name of the column to use for the main query
#' @param property_cols A vector of column names to use for additional
#'   properties (optional)
#' @param type The type of entity to reconcile against (optional)
#' @param match_limit The maximum number of reconciliation matches to return for
#'   each candidate (optional)
#' @param query_limit The maximum number of candidates to submit with each
#'   query (optional)—defaults to 10. Try a lower number if the API returns a 413 Payload
#'   Too Large response
#' @param matches_only Whether to return reconciliation matches on their own,
#'   without candidate data (optional)—defaults to "FALSE"
#' @return The original data frame with reconciliation matches as additional
#'   columns, or a data frame of matches on their own if matches_only is
#'   "TRUE".
#' @examples
#' gw_companies <- tribble(
#'   ~name, ~jurisdiction_code, ~country_code,
#'   "Global Witness", "gb", "gb",
#'   "Global Witness", "be", "be",
#'   "Global Witness", "us_ca", "us")
#'
#' gw_companies %>%
#'   reconcile(
#'     endpoint = "https://reconcile.opencorporates.com?api_token=TOKEN",
#'     query_col = "name",
#'     property_cols = c("jurisdiction_code", "country_code")) %>%
#'  filter(match_score >= 75)
reconcile <- function(data, endpoint, query_col, property_cols = NULL, type = NULL, match_limit = NULL, query_limit = 10, matches_only = FALSE) {

  data %>%
    split(rep(1:ceiling(nrow(data) / query_limit), each = query_limit)[1:nrow(data)]) %>%
    map_dfr(function(chunk) {

      cat(glue("Reconciling candidates \"{chunk[[query_col]][1]}\" to \"{chunk[[query_col]][nrow(chunk)]}\"... "))

      payload <- build_query(chunk, query_col, property_cols, type, match_limit)
      results <- payload %>%
        send_request(endpoint) %>%
        parse_results()

      cat("✓\n")

      if(matches_only) {
        results
      } else {
        chunk %>%
          as_tibble() %>%
          mutate(query_id = names(payload)) %>%
          left_join(
            rename_at(results, vars(-query_id), ~str_c("match_", .x)),
            by = "query_id") %>%
          select(-query_id)
      }
    })
}
