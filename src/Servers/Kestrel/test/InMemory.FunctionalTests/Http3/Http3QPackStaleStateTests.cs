// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Threading;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.AspNetCore.InternalTesting;
using Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Http3;
using Xunit;

namespace Microsoft.AspNetCore.Server.Kestrel.Core.Tests;

public class Http3QPackStaleStateTests : Http3TestBase
{
    [Fact]
    public async Task IncompleteTrailerHeaders_DoesNotCorruptNextPooledRequest()
    {
        var requestCount = 0;
        var secondRequestProcessed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondRequestHasA = false;
        var secondRequestA = string.Empty;

        await Http3Api.InitializeConnectionAsync(context =>
        {
            if (Interlocked.Increment(ref requestCount) == 2)
            {
                secondRequestHasA = context.Request.Headers.TryGetValue("a", out var value);
                secondRequestA = value.ToString();
                secondRequestProcessed.TrySetResult();
            }

            return Task.CompletedTask;
        });

        await Http3Api.CreateControlStream();
        await Http3Api.GetInboundControlStream();

        var firstHeaders = new[]
        {
            new KeyValuePair<string, string>(InternalHeaderNames.Method, "GET"),
            new KeyValuePair<string, string>(InternalHeaderNames.Path, "/"),
            new KeyValuePair<string, string>(InternalHeaderNames.Scheme, "http"),
            new KeyValuePair<string, string>(InternalHeaderNames.Authority, "localhost:80"),
        };

        // Complete the initial request headers and let the application respond.
        // Keep the request stream open so an incomplete trailer block can follow.
        var first = await Http3Api.CreateRequestStream(firstHeaders);
        await first.ExpectHeadersAsync();

        var firstServerStream = (Http3Stream)first.StreamContext.Features
            .Get<IPersistentStateFeature>().State[Http3Connection.StreamPersistentStateKey];

        // QPACK prefix (00 00), then a literal field using static name index 0
        // (:authority), with its value missing. This leaves the static index pending.
        await first.SendTrailerHeadersPartialAndEndStreamAsync(
            new byte[] { 0x00, 0x00, 0x50 },
            declaredLength: 100);

        // Wait for the first stream to be disposed and its transport context returned.
        await first.OnDisposedTask.DefaultTimeout();
        Assert.True(firstServerStream.CanReuse,
            "The first Http3Stream was not eligible for pooling after the incomplete trailer block.");

        // Create a second request on the same connection using a raw QPACK block.
        var second = await Http3Api.CreateRequestStream(Array.Empty<KeyValuePair<string, string>>());
        Assert.Same(first.StreamContext, second.StreamContext);

        byte[] secondRequestHeaderBlock =
        {
            0x00, 0x00,                         // Required Insert Count = 0, Base = 0
            0xD1,                               // :method: GET (static index 17)
            0xD6,                               // :scheme: http (static index 22)
            0xC1,                               // :path: / (static index 1)
            0x21, 0x61,                         // Literal header name: "a"
            0x0B,                               // Literal value length: 11
            (byte)'e', (byte)'x', (byte)'a', (byte)'m', (byte)'p', (byte)'l',
            (byte)'e', (byte)'.', (byte)'c', (byte)'o', (byte)'m'
        };

        await second.SendHeadersAsync(secondRequestHeaderBlock, endStream: true);
        await secondRequestProcessed.Task.DefaultTimeout();

        var secondServerStream = (Http3Stream)second.StreamContext.Features
            .Get<IPersistentStateFeature>().State[Http3Connection.StreamPersistentStateKey];

        // Ensure the actual Kestrel Http3Stream (and therefore its QPackDecoder)
        // was reused, not just the in-memory transport wrapper.
        Assert.Same(firstServerStream, secondServerStream);

        // A stale :authority index must not replace the literal "a" header.
        Assert.True(secondRequestHasA, "The second request lost its literal 'a' header.");
        Assert.Equal("example.com", secondRequestA);

        await second.ExpectHeadersAsync();
        await second.ExpectReceiveEndOfStream();
        await second.OnDisposedTask.DefaultTimeout();
    }
}
