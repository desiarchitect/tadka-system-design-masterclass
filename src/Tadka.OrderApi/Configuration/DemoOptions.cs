namespace Tadka.OrderApi.Configuration;

/// <summary>
/// Demo-only switches. Bound from the "Demo" section, so they can be changed with
/// appsettings.Demo.json or an environment variable such as Demo__HoldMs=20.
/// </summary>
public sealed class DemoOptions
{
    public const string Section = "Demo";

    /// <summary>
    /// Artificial delay, in milliseconds, run inside the database command with pg_sleep.
    /// It keeps a pooled connection busy without using database CPU.
    /// It is NOT a simulation of a slow production query. 0 turns it off.
    /// </summary>
    public int HoldMs { get; set; }
}
