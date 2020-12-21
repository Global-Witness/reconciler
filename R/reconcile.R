build_query <- function(query, properties, limit) {
  payload <- list(query = query)

  if(!missing(properties)) {
    payload$properties <- map2(names(properties), properties,
      function(pid, v) {
        list(pid = pid, v = v)
      }
    )
  }

  if(!missing(limit)) {
    payload$limit <- limit
  }

  payload
}

get_results <- function(endpoint, payload) {
  response <-
    GET(
      url = endpoint,
      query = list(queries = toJSON(payload, auto_unbox = TRUE)))

  response %>%
    content("text", encoding = "UTF-8") %>%
    fromJSON(simplifyVector = FALSE)
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
#' @param limit The maximum number of reconciliation matches to return for each
#'   candidate—defaults to 10
#' @param matches_only Whether to return reconciliation matches on their own,
#'   without candidate data—defaults to "FALSE"
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
reconcile <- function(data, endpoint, query_col, property_cols = NULL, limit = 10, matches_only = FALSE) {
  payload <- data %>%
    select(query = query_col, property_cols) %>%
    pmap(function(...) {
      row <- list(...)
      properties <- row[names(row) != "query"]
      if(length(properties) > 0) {
        build_query(
          query = row$query,
          properties = properties,
          limit = limit)
      } else {
        build_query(
          query = row$query,
          limit = limit)
      }
    })

  names(payload) <-
    seq(0, length(payload) - 1) %>%
    map_chr(~str_c("q", .x))

  results <-
    get_results(endpoint, payload) %>%
    map_dfr(tibble, .id = "query_id") %>%
    unnest_longer(`<list>`) %>%
    unnest_wider(`<list>`)

  if(matches_only) {
    results
  } else {
    data %>%
      as_tibble() %>%
      mutate(query_id = names(payload)) %>%
      left_join(
        rename_at(results, vars(-query_id), ~str_c("match_", .x)),
        by = "query_id")
  }
}
