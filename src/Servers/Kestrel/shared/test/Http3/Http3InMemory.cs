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

        // Request 1 completes its initial HEADERS frame and its response.
        // The request stream itself remains open so that we can send incomplete trailers.
        var first = await Http3Api.CreateRequestStream(firstHeaders);
        await first.ExpectHeadersAsync();

        var firstServerStream = (Http3Stream)first.StreamContext.Features
            .Get<IPersistentStateFeature>().State[Http3Connection.StreamPersistentStateKey];

        // QPACK prefix (00 00), followed by a literal field with a static name reference
        // to index 0 (:authority), but without its value. This leaves _headerStaticIndex
        // set when the request stream reaches FIN.
        await first.SendTrailerHeadersPartialAndEndStreamAsync(
            new byte[] { 0x00, 0x00, 0x50 },
            declaredLength: 100);

        // Wait until the transport context has been returned to the in-memory stream pool.
        await first.OnDisposedTask.DefaultTimeout();
        Assert.True(firstServerStream.CanReuse, "The first Http3Stream was not eligible for pooling.");

        // Create another request on the same pooled transport context. Its first fields
        // are indexed pseudo-headers; the final literal field is "a: example.com".
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

        await second.SendFrameAsync(Http3FrameType.Headers, secondRequestHeaderBlock, endStream: true);
        await secondRequestProcessed.Task.DefaultTimeout();

        var secondServerStream = (Http3Stream)second.StreamContext.Features
            .Get<IPersistentStateFeature>().State[Http3Connection.StreamPersistentStateKey];

        // Confirm that the server reused the same Http3Stream instance, not merely
        // the in-memory transport wrapper.
        Assert.Same(firstServerStream, secondServerStream);

        // Correct behavior: the stale :authority index from request 1 must not replace
        // the literal "a" header in request 2.
        Assert.True(secondRequestHasA, "The second request lost its literal 'a' header.");
        Assert.Equal("example.com", secondRequestA);

        await second.ExpectHeadersAsync();
        await second.ExpectReceiveEndOfStream();
        await second.OnDisposedTask.DefaultTimeout();
    }
}
