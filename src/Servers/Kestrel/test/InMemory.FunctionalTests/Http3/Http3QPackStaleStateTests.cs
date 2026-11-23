
// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Net.Http;
using System;
using System.Collections.Generic;
using System.Threading;
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

        var secondRequestProcessed = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);

        var secondRequestHasA = false;
        var secondRequestA = string.Empty;

        await Http3Api.InitializeConnectionAsync(async context =>
        {
            if (Interlocked.Increment(ref requestCount) == 1)
            {
                // Keep the first request active until the incomplete trailers arrive.
                var buffer = new byte[1024];

                while (await context.Request.Body.ReadAsync(
                    buffer, 0, buffer.Length) != 0)
                {
                }
            }
            else
            {
                secondRequestHasA =
                    context.Request.Headers.TryGetValue("a", out var value);

                secondRequestA = value.ToString();
                secondRequestProcessed.TrySetResult();
            }
        });

        await Http3Api.CreateControlStream();
        await Http3Api.GetInboundControlStream();

        var firstHeaders = new[]
        {
            new KeyValuePair<string, string>(
                InternalHeaderNames.Method, "GET"),

            new KeyValuePair<string, string>(
                InternalHeaderNames.Path, "/"),

            new KeyValuePair<string, string>(
                InternalHeaderNames.Scheme, "http"),

            new KeyValuePair<string, string>(
                InternalHeaderNames.Authority, "localhost:80"),
        };

        var first = await Http3Api.CreateRequestStream(firstHeaders);

        // Start a literal using static name index 0 (:authority),
        // without sending its value.
        await first.SendTrailerHeadersPartialAndEndStreamAsync(
            new byte[] { 0x00, 0x00, 0x50 },
            declaredLength: 100);

        // Wait until Kestrel has processed the incomplete trailer block
        // and completed the first response.
        await first.ExpectHeadersAsync();
        await first.OnDisposedTask.DefaultTimeout();

        // The workflow logs will show whether Kestrel pooled this stream.

        // Create another request on the same HTTP/3 connection.
        var second = await Http3Api.CreateRequestStream(
            Array.Empty<KeyValuePair<string, string>>());

        byte[] secondRequestHeaderBlock =
        {
            0x00, 0x00,

            0xD1, // :method GET
            0xD6, // :scheme http
            0xC1, // :path /

            0x21, 0x61, // Literal header name "a"
            0x0B, // Value length: 11

            (byte)'e',
            (byte)'x',
            (byte)'a',
            (byte)'m',
            (byte)'p',
            (byte)'l',
            (byte)'e',
            (byte)'.',
            (byte)'c',
            (byte)'o',
            (byte)'m'
        };

        await second.SendFrameAsync(
            Http3FrameType.Headers,
            secondRequestHeaderBlock,
            endStream: true);

        // Wait until the application has received the second request.
        await secondRequestProcessed.Task.DefaultTimeout();

        // The second request must retain its literal header name.
        Assert.True(
            secondRequestHasA,
            "The second request lost its literal 'a' header.");

        Assert.Equal("example.com", secondRequestA);

        await second.ExpectHeadersAsync();
        await second.ExpectReceiveEndOfStream();
        await second.OnDisposedTask.DefaultTimeout();
    }
}
