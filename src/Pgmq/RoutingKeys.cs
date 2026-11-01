namespace Pgmq;

/// <summary>
/// The routing key rules of <c>pgmq.validate_routing_key</c>, checked before anything is sent, so a key that can never
/// go out fails where it is passed instead of in the database.
/// </summary>
internal static class RoutingKeys
{
    private const int MaxLength = 255;

    /// <summary>
    /// Throws unless <paramref name="routingKey"/> is 1 to 255 letters, digits, dots, hyphens and underscores, with no
    /// dot at either end and no two dots in a row: <c>orders.eu.created</c>, not <c>orders..created</c> or
    /// <c>orders.big spender</c>. Wildcards belong in topic patterns, not in routing keys.
    /// </summary>
    public static void ThrowIfInvalid(string routingKey, string paramName = "routingKey")
    {
        ArgumentException.ThrowIfNullOrEmpty(routingKey, paramName);

        string? problem = routingKey.Length > MaxLength ? $"is longer than {MaxLength} characters"
            : !routingKey.All(IsAllowed) ? "may only contain letters, digits, '.', '-' and '_'"
            : routingKey[0] == '.' ? "cannot start with a dot"
            : routingKey[^1] == '.' ? "cannot end with a dot"
            : routingKey.Contains("..", StringComparison.Ordinal) ? "cannot contain two dots in a row"
            : null;

        if (problem is not null)
        {
            throw new ArgumentException($"The routing key '{routingKey}' {problem}.", paramName);
        }
    }

    private static bool IsAllowed(char c) => char.IsAsciiLetterOrDigit(c) || c is '.' or '-' or '_';
}
