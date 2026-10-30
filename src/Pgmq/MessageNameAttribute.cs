namespace Pgmq;

/// <summary>
/// Pins the name a message type travels under. By default it is the type's full name, so renaming or moving the
/// class changes it, and messages already queued under the old name are no longer understood. Before renaming or
/// moving a message type, put its current name here.
/// </summary>
/// <param name="name">The name written into every message of this type.</param>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Interface, Inherited = false)]
public sealed class MessageNameAttribute(string name) : Attribute
{
    /// <summary>The name written into every message of this type.</summary>
    public string Name { get; } = name;
}
