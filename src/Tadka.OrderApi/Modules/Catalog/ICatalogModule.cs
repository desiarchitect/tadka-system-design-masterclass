namespace Tadka.OrderApi.Modules.Catalog;

/// <summary>Public contract exposed by the Catalog module inside the modular monolith.</summary>
public sealed record CatalogMenuItem(
    long Id,
    long RestaurantId,
    string Name,
    decimal Price,
    bool IsAvailable,
    long Version);

public sealed record CatalogMenuResponse(
    long RestaurantId,
    string RestaurantName,
    bool IsOpen,
    IReadOnlyList<CatalogMenuItem> Items);

public sealed record RequestedOrderItem(long MenuItemId, int Quantity);

public sealed record QuotedOrderItem(
    long MenuItemId,
    string Name,
    decimal UnitPrice,
    int Quantity,
    decimal LineTotal);

public sealed record CatalogQuoteResult(
    bool RestaurantFound,
    bool Conflict,
    string? ErrorReason,
    string? RestaurantName,
    decimal TotalAmount,
    IReadOnlyList<QuotedOrderItem> Items);

public interface ICatalogModule
{
    Task<CatalogMenuResponse?> GetMenuAsync(long restaurantId, CancellationToken ct);
    Task<CatalogQuoteResult> QuoteItemsAsync(long restaurantId, IReadOnlyList<RequestedOrderItem> items, CancellationToken ct);
    Task<CatalogMenuItem?> UpdateItemAsync(long restaurantId, long itemId, decimal? price, bool? isAvailable, CancellationToken ct);
}
