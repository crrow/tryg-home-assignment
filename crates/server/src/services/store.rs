// Copyright 2025 Crrow
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use rsketch_api::pb::store::v1::{GetRequest, GetResponse, SetRequest};
use tonic::{Request, Response, Status, service::RoutesBuilder};

use crate::grpc::GrpcServiceHandler;

#[derive(Debug, Default)]
pub struct StoreSvc {}

impl StoreSvc {
    pub fn new() -> Self { Self {} }
}

#[async_trait::async_trait]
impl rsketch_api::pb::store::v1::store_server::Store for StoreSvc {
    async fn get(&self, _request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        // TODO: Implement get functionality
        Err(tonic::Status::unimplemented("get not implemented"))
    }

    async fn set(&self, _request: Request<SetRequest>) -> Result<Response<()>, Status> {
        // TODO: Implement set functionality
        Err(tonic::Status::unimplemented("set not implemented"))
    }
}

#[async_trait::async_trait]
impl GrpcServiceHandler for StoreSvc {
    fn service_name(&self) -> &'static str { "Store" }

    fn file_descriptor_set(&self) -> &'static [u8] { rsketch_api::pb::GRPC_DESC }

    fn register_service(self: &Arc<Self>, builder: &mut RoutesBuilder) {
        use tonic::service::LayerExt as _;
        let svc = tower::ServiceBuilder::new()
            .layer(tower_http::cors::CorsLayer::new())
            .layer(tonic_web::GrpcWebLayer::new())
            .into_inner()
            .named_layer(
                rsketch_api::pb::store::v1::store_server::StoreServer::from_arc(self.clone()),
            );
        builder.add_service(svc);
    }
}
