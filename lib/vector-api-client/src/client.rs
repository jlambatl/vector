use anyhow::Context;
use graphql_client::GraphQLQuery;
use url::Url;

use crate::gql::HealthQueryExt;

/// Wrapped `Result` type, that returns deserialized GraphQL response data.
pub type QueryResult<T> =
    anyhow::Result<graphql_client::Response<<T as GraphQLQuery>::ResponseData>>;

/// GraphQL query client over HTTP.
#[derive(Debug)]
pub struct Client {
    url: Url,
    api_token: Option<String>,
}

impl Client {
    /// Returns a new GraphQL query client, bound to the provided URL.
    pub fn new(url: Url) -> Self {
        Self {
            url,
            api_token: None,
        }
    }

    pub fn with_token(url: Url, token: String) -> Self {
        Self {
            url,
            api_token: Some(token),
        }
    }

    /// Send a health query
    pub async fn healthcheck(&self) -> Result<(), ()> {
        self.health_query().await.map(|_| ()).map_err(|_| ())
    }

    /// Issue a GraphQL query using Reqwest, serializing the response to the associated
    /// GraphQL type for the given `request_body`.
    pub async fn query<T: GraphQLQuery>(
        &self,
        request_body: &graphql_client::QueryBody<T::Variables>,
    ) -> QueryResult<T> {
        let client = reqwest::Client::new();

        let mut request = client.post(self.url.clone()).json(request_body);

        if let Some(token) = &self.api_token {
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::AUTHORIZATION,
                token.to_string().parse().unwrap(),
            );
            request = request.headers(headers);
        }

        request
            .send()
            .await
            .with_context(|| {
                format!(
                    "Couldn't send '{}' query to {}",
                    request_body.operation_name,
                    &self.url.as_str()
                )
            })?
            .json()
            .await
            .with_context(|| {
                format!(
                    "Couldn't serialize the response for '{}' query: {:?}",
                    request_body.operation_name, request_body.query
                )
            })
    }
}
