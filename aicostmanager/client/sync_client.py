from __future__ import annotations

import os
from typing import Any, Dict, Iterable, Iterator, List, Optional

import requests

from ..models import (
    ApiCostEventOut,
    CostEventFilters,
    CostEventsResponse,
    CostUnitOut,
    CustomerFilters,
    CustomerIn,
    CustomerOut,
    CustomServiceFilter,
    CustomServiceIn,
    CustomServiceOut,
    CustomServiceSummaryOut,
    PaginatedResponse,
    RollupFilters,
    ServiceOut,
    UsageEvent,
    UsageEventFilters,
    UsageLimitIn,
    UsageLimitOut,
    UsageLimitProgressOut,
    UsageRollup,
    VendorOut,
    WebhookEndpointCreate,
    WebhookEndpointOut,
    WebhookEndpointsResponse,
    WebhookEndpointUpdate,
)
from .base import BaseClient
from .exceptions import APIRequestError


class CostManagerClient(BaseClient):
    """Client for AICostManager endpoints."""

    def __init__(
        self,
        *,
        aicm_api_key: Optional[str] = None,
        aicm_api_base: Optional[str] = None,
        aicm_api_url: Optional[str] = None,
        aicm_ini_path: Optional[str] = None,
        session: Optional[requests.Session] = None,
        proxies: Optional[dict[str, str]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        super().__init__(
            aicm_api_key=aicm_api_key,
            aicm_api_base=aicm_api_base,
            aicm_api_url=aicm_api_url,
            aicm_ini_path=aicm_ini_path,
        )
        if session is None:
            session = requests.Session()
            if proxies:
                setattr(session, "proxies", getattr(session, "proxies", {}))
                session.proxies.update(proxies)
        elif proxies:
            setattr(session, "proxies", getattr(session, "proxies", {}))
            session.proxies.update(proxies)
        self.session = session
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.api_key}",
                "User-Agent": "aicostmanager-python",
            }
        )
        if headers:
            self.session.headers.update(headers)

        # Initialize triggered limits during client instantiation
        self._initialize_triggered_limits()

    def close(self) -> None:
        """Close the underlying requests session."""
        self.session.close()

    def __enter__(self) -> "CostManagerClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _initialize_triggered_limits(self) -> None:
        """Initialize triggered limits during client instantiation."""
        try:
            if self.ini_path == "ini" or os.path.basename(self.ini_path) == "ini":
                return
            try:
                triggered_limits_response = self.get_triggered_limits()
                self._store_triggered_limits(triggered_limits_response)
            except Exception:
                pass
        except Exception:
            pass

    # internal helper
    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        url = path if path.startswith("http") else self.api_root + path
        resp = self.session.request(method, url, **kwargs)
        if not resp.ok:
            try:
                detail = resp.json()
            except Exception:
                detail = resp.text
            raise APIRequestError(resp.status_code, detail)
        if resp.status_code == 204:
            return None
        return resp.json()

    def _iter_paginated(self, path: str, **params: Any) -> Iterator[dict]:
        while True:
            data = self._request("GET", path, params=params)
            for item in data.get("results", []):
                yield item
            next_url = data.get("next")
            if not next_url:
                break
            if next_url.startswith(self.api_root):
                path = next_url[len(self.api_root) :]
            else:
                path = next_url
            params = {}

    # endpoint methods

    def get_triggered_limits(self) -> Dict[str, Any]:
        """Fetch triggered limit information from the API."""
        return self._request("GET", "/triggered-limits")

    def list_usage_events(
        self,
        filters: UsageEventFilters | Dict[str, Any] | None = None,
        **params: Any,
    ) -> Any:
        if filters:
            if hasattr(filters, "model_dump"):
                params.update(filters.model_dump(exclude_none=True))
            else:
                params.update({k: v for k, v in filters.items() if v is not None})
        return self._request("GET", "/usage/events/", params=params)

    def list_usage_events_typed(
        self,
        filters: UsageEventFilters | Dict[str, Any] | None = None,
        **params: Any,
    ) -> PaginatedResponse[UsageEvent]:
        """Typed variant of :meth:`list_usage_events`."""
        data = self.list_usage_events(filters, **params)
        return PaginatedResponse[UsageEvent].model_validate(data)

    def iter_usage_events(
        self,
        filters: UsageEventFilters | Dict[str, Any] | None = None,
        **params: Any,
    ) -> Iterator[UsageEvent]:
        if filters:
            if hasattr(filters, "model_dump"):
                params.update(filters.model_dump(exclude_none=True))
            else:
                params.update({k: v for k, v in filters.items() if v is not None})
        for item in self._iter_paginated("/usage/events/", **params):
            yield UsageEvent.model_validate(item)

    def get_usage_event(self, event_id: str) -> UsageEvent:
        data = self._request("GET", f"/usage/event/{event_id}/")
        return UsageEvent.model_validate(data)

    def list_usage_rollups(
        self,
        filters: RollupFilters | Dict[str, Any] | None = None,
        **params: Any,
    ) -> Any:
        if filters:
            if hasattr(filters, "model_dump"):
                params.update(filters.model_dump(exclude_none=True))
            else:
                params.update({k: v for k, v in filters.items() if v is not None})
        return self._request("GET", "/usage/rollups/", params=params)

    def list_usage_rollups_typed(
        self,
        filters: RollupFilters | Dict[str, Any] | None = None,
        **params: Any,
    ) -> PaginatedResponse[UsageRollup]:
        """Typed variant of :meth:`list_usage_rollups`."""
        data = self.list_usage_rollups(filters, **params)
        return PaginatedResponse[UsageRollup].model_validate(data)

    def iter_usage_rollups(
        self,
        filters: RollupFilters | Dict[str, Any] | None = None,
        **params: Any,
    ) -> Iterator[UsageRollup]:
        if filters:
            if hasattr(filters, "model_dump"):
                params.update(filters.model_dump(exclude_none=True))
            else:
                params.update({k: v for k, v in filters.items() if v is not None})
        for item in self._iter_paginated("/usage/rollups/", **params):
            yield UsageRollup.model_validate(item)

    def list_customers(
        self,
        filters: CustomerFilters | Dict[str, Any] | None = None,
        **params: Any,
    ) -> Any:
        if filters:
            if hasattr(filters, "model_dump"):
                params.update(filters.model_dump(exclude_none=True))
            else:
                params.update({k: v for k, v in filters.items() if v is not None})
        return self._request("GET", "/customers/", params=params)

    def list_customers_typed(
        self,
        filters: CustomerFilters | Dict[str, Any] | None = None,
        **params: Any,
    ) -> PaginatedResponse[CustomerOut]:
        """Typed variant of :meth:`list_customers`."""
        data = self.list_customers(filters, **params)
        return PaginatedResponse[CustomerOut].model_validate(data)

    def iter_customers(self, **params: Any) -> Iterator[CustomerOut]:
        for item in self._iter_paginated("/customers/", **params):
            yield CustomerOut.model_validate(item)

    def create_customer(self, data: CustomerIn | Dict[str, Any]) -> CustomerOut:
        payload = data.model_dump(mode="json") if isinstance(data, CustomerIn) else data
        resp = self._request("POST", "/customers/", json=payload)
        return CustomerOut.model_validate(resp)

    def get_customer(self, customer_id: str) -> CustomerOut:
        data = self._request("GET", f"/customers/{customer_id}/")
        return CustomerOut.model_validate(data)

    def update_customer(
        self, customer_id: str, data: CustomerIn | Dict[str, Any]
    ) -> CustomerOut:
        payload = data.model_dump(mode="json") if isinstance(data, CustomerIn) else data
        resp = self._request("PUT", f"/customers/{customer_id}/", json=payload)
        return CustomerOut.model_validate(resp)

    def delete_customer(self, customer_id: str) -> None:
        self._request("DELETE", f"/customers/{customer_id}/")
        return None

    def list_usage_limits(self) -> Iterable[UsageLimitOut]:
        data = self._request("GET", "/usage-limits/")
        return [UsageLimitOut.model_validate(i) for i in data]

    def create_usage_limit(self, data: UsageLimitIn | Dict[str, Any]) -> UsageLimitOut:
        payload = (
            data.model_dump(mode="json") if isinstance(data, UsageLimitIn) else data
        )
        resp = self._request("POST", "/usage-limits/", json=payload)
        return UsageLimitOut.model_validate(resp)

    def get_usage_limit(self, limit_id: str) -> UsageLimitOut:
        data = self._request("GET", f"/usage-limits/{limit_id}/")
        return UsageLimitOut.model_validate(data)

    def update_usage_limit(
        self, limit_id: str, data: UsageLimitIn | Dict[str, Any]
    ) -> UsageLimitOut:
        payload = (
            data.model_dump(mode="json") if isinstance(data, UsageLimitIn) else data
        )
        resp = self._request("PUT", f"/usage-limits/{limit_id}/", json=payload)
        return UsageLimitOut.model_validate(resp)

    def delete_usage_limit(self, limit_id: str) -> None:
        self._request("DELETE", f"/usage-limits/{limit_id}/")
        return None

    def list_usage_limit_progress(self) -> Iterable[UsageLimitProgressOut]:
        data = self._request("GET", "/usage-limits/progress/")
        return [UsageLimitProgressOut.model_validate(i) for i in data]

    def list_vendors(self) -> Iterable[VendorOut]:
        data = self._request("GET", "/vendors/")
        return [VendorOut.model_validate(i) for i in data]

    def list_vendor_services(self, vendor: str) -> Iterable[ServiceOut]:
        data = self._request("GET", "/services/", params={"vendor": vendor})
        # Add vendor field to each service object since the API doesn't include it
        for service in data:
            service["vendor"] = vendor
        return [ServiceOut.model_validate(i) for i in data]

    def list_service_costs(self, vendor: str, service: str) -> Iterable[CostUnitOut]:
        """List cost units for a service."""
        data = self._request(
            "GET",
            "/service-costs/",
            params={"vendor": vendor, "service": service},
        )
        return [CostUnitOut.model_validate(i) for i in data]

    # Cost events methods
    def list_cost_events(
        self,
        filters: CostEventFilters | Dict[str, Any] | None = None,
        **params: Any,
    ) -> CostEventsResponse:
        if filters:
            if hasattr(filters, "model_dump"):
                params.update(filters.model_dump(exclude_none=True))
            else:
                params.update({k: v for k, v in filters.items() if v is not None})
        data = self._request("GET", "/costs/", params=params)
        return CostEventsResponse.model_validate(data)

    def list_cost_events_by_response_id(
        self, response_id: str
    ) -> List[ApiCostEventOut]:
        data = self._request("GET", f"/cost-events/{response_id}")
        return [ApiCostEventOut.model_validate(i) for i in data]

    # Webhook methods
    def create_webhook_endpoint(
        self, data: WebhookEndpointCreate | Dict[str, Any]
    ) -> WebhookEndpointOut:
        payload = (
            data.model_dump(mode="json")
            if isinstance(data, WebhookEndpointCreate)
            else data
        )
        resp = self._request("POST", "/webhooks/", json=payload)
        return WebhookEndpointOut.model_validate(resp)

    def list_webhook_endpoints(
        self, active_only: bool = False, **params: Any
    ) -> WebhookEndpointsResponse:
        params = {k: v for k, v in params.items() if v is not None}
        params["active_only"] = active_only
        data = self._request("GET", "/webhooks/", params=params)
        return WebhookEndpointsResponse.model_validate(data)

    def get_webhook_endpoint(self, webhook_uuid: str) -> WebhookEndpointOut:
        data = self._request("GET", f"/webhooks/{webhook_uuid}/")
        return WebhookEndpointOut.model_validate(data)

    def update_webhook_endpoint(
        self, webhook_uuid: str, data: WebhookEndpointUpdate | Dict[str, Any]
    ) -> WebhookEndpointOut:
        payload = (
            data.model_dump(mode="json", exclude_none=True)
            if isinstance(data, WebhookEndpointUpdate)
            else data
        )
        resp = self._request("PUT", f"/webhooks/{webhook_uuid}/", json=payload)
        return WebhookEndpointOut.model_validate(resp)

    def delete_webhook_endpoint(self, webhook_uuid: str) -> None:
        self._request("DELETE", f"/webhooks/{webhook_uuid}/")
        return None

    # Custom services methods
    def list_custom_services(
        self,
        filters: CustomServiceFilter | Dict[str, Any] | None = None,
        **params: Any,
    ) -> List[CustomServiceSummaryOut]:
        if filters:
            if hasattr(filters, "model_dump"):
                params.update(filters.model_dump(exclude_none=True))
            else:
                params.update({k: v for k, v in filters.items() if v is not None})
        data = self._request("GET", "/custom-services/", params=params)
        return [CustomServiceSummaryOut.model_validate(i) for i in data]

    def create_custom_service(
        self, data: CustomServiceIn | Dict[str, Any]
    ) -> CustomServiceOut:
        payload = (
            data.model_dump(mode="json") if isinstance(data, CustomServiceIn) else data
        )
        resp = self._request("POST", "/custom-services/", json=payload)
        return CustomServiceOut.model_validate(resp)

    def get_custom_service(self, uuid: str) -> CustomServiceOut:
        data = self._request("GET", f"/custom-services/{uuid}/")
        return CustomServiceOut.model_validate(data)

    def update_custom_service(
        self, uuid: str, data: CustomServiceIn | Dict[str, Any]
    ) -> CustomServiceOut:
        payload = (
            data.model_dump(mode="json") if isinstance(data, CustomServiceIn) else data
        )
        resp = self._request("PUT", f"/custom-services/{uuid}/", json=payload)
        return CustomServiceOut.model_validate(resp)

    def delete_custom_service(self, uuid: str) -> None:
        self._request("DELETE", f"/custom-services/{uuid}/")
        return None

    def get_openapi_schema(self) -> Any:
        return self._request("GET", "/openapi.json")
