package dev.loams.test

import com.google.protobuf.DynamicMessage
import dev.loams.CallOptions
import dev.loams.Descriptors
import dev.loams.Facade
import dev.loams.IdempotencyLevel
import dev.loams.ReasonRegistry
import dev.loams.RetryClass
import dev.loams.StaticTokenSource
import dev.loams.test.Support.StubTransport

/**
 * `loams.live.admin`'s app-directory calls (LV1 Task 13): the five calls are derived from
 * the descriptor set, reach `LiveAdminService`, and `CallOptions.liveApp` puts the app on
 * the wire as the `loams-live-app` header.
 */
object LiveAdminTest {
    private const val ADMIN = "loams.live.v1.LiveAdminService"
    private val apps = listOf("createApp", "deleteApp", "getApp", "listApps", "updateApp")

    private fun empty(name: String): DynamicMessage {
        val descriptor = requireNotNull(Descriptors.message("loams.live.v1.${name}Request"))
        return DynamicMessage.getDefaultInstance(descriptor)
    }

    fun register() {
        Harness.test("kotlin_live_admin_the_app_calls_are_in_the_catalogue") {
            val module = Facade.module("live.admin")
            assertEquals(ADMIN, module.service, "the service")
            assertTrue(module.unstable, "live.admin to be marked unstable")
            val calls = module.calls.map { it.facadeName }.toSet()
            for (call in apps) {
                assertTrue(call in calls, "live.admin to have $call (has $calls)")
            }
            for (call in module.calls) {
                assertEquals("$ADMIN/${call.method}", call.rpc, "${call.facadeName}'s rpc")
                assertTrue(!call.takesIdempotencyKey, "${call.facadeName} to take no idempotency key")
            }
            val safe = setOf("getApp", "listApps")
            for (name in apps) {
                val binding = Facade.binding("live.admin", name)
                val want = if (name in safe) RetryClass.SAFE else RetryClass.MANUAL
                assertEquals(want, binding.retry, "$name's retry class")
                assertEquals(IdempotencyLevel.NONE, binding.idempotency, "$name's idempotency level")
            }
            assertEquals("apps:next_page_token", Facade.binding("live.admin", "listApps").pagination, "listApps paging")
        }

        Harness.test("kotlin_live_admin_each_call_reaches_the_admin_service_with_the_app_header") {
            val transport = StubTransport()
            for (name in apps) {
                val binding = Facade.binding("live.admin", name)
                transport.answerMessage(binding.response, DynamicMessage.getDefaultInstance(binding.response))
            }
            // Answers are consumed in order, so the calls are made in the answers' order.
            val admin = transport.client().modules.liveAdmin
            val blog = CallOptions.liveApp("blog")
            admin.invoke("createApp", empty("CreateApp"))
            admin.invoke("deleteApp", empty("DeleteApp"), blog)
            admin.invoke("getApp", empty("GetApp"), blog)
            admin.invoke("listApps", empty("ListApps"))
            admin.invoke("updateApp", empty("UpdateApp"), blog)

            val sent = transport.requests
            assertEquals(
                apps.map { "$ADMIN/${it.replaceFirstChar(Char::uppercase)}" },
                sent.map { it.rpc },
                "the rpcs sent",
            )
            assertEquals(
                listOf(null, "blog", "blog", null, "blog"),
                sent.map { it.headers[CallOptions.LIVE_APP_HEADER] },
                "the loams-live-app header on each call",
            )
            assertEquals("loams-live-app", CallOptions.LIVE_APP_HEADER, "the header's name")
        }

        Harness.test("kotlin_live_admin_liveApp_keeps_the_other_overrides") {
            val options = CallOptions.liveApp("blog", CallOptions(maxRetries = 0, headers = mapOf("x-trace" to "1")))
            assertEquals(0, options.maxRetries, "maxRetries")
            assertEquals(mapOf("x-trace" to "1", "loams-live-app" to "blog"), options.headers, "headers")
        }

        Harness.test("kotlin_live_admin_the_live_app_reasons_are_in_the_registry") {
            for (wire in listOf("live_app_disabled", "live_app_exists", "live_app_mismatch")) {
                assertTrue(ReasonRegistry.fromWire(wire) != null, "$wire to be a known reason")
            }
        }
        Harness.test("kotlin_live_authenticate_is_in_the_catalogue_unary_and_never_retried") {
            val binding = Facade.binding("live", "authenticate")
            assertEquals("loams.live.v1.LiveService/Authenticate", binding.rpc, "the rpc")
            assertEquals(RetryClass.MANUAL, binding.retry, "the retry class")
            assertTrue(!binding.takesIdempotencyKey, "authenticate to take no idempotency key")
        }

        Harness.test("kotlin_live_authenticate_sends_the_session_and_bearer_and_surfaces_both_outcomes") {
            val binding = Facade.binding("live", "authenticate")
            fun identity(version: Long): DynamicMessage = DynamicMessage.newBuilder(binding.response)
                .setField(binding.response.findFieldByName("identity"), version)
                .build()
            // 4 is the version the session holds: the same caller answers 4 again (the
            // expiry moved), another caller answers 5 (the identity changed).
            val transport = StubTransport()
            transport.answerMessage(binding.response, identity(4))
            transport.answerMessage(binding.response, identity(5))
            val live = transport.client(tokenSource = StaticTokenSource("Bearer tok-bob")).modules.live

            val extended = live.authenticate("s1")
            val changed = live.authenticate("s1")

            assertEquals(4L, live.identityOf(extended), "the same caller leaves the version unchanged")
            assertEquals(5L, live.identityOf(changed), "another caller bumps it")
            assertEquals(
                listOf("loams.live.v1.LiveService/Authenticate", "loams.live.v1.LiveService/Authenticate"),
                transport.requests.map { it.rpc },
                "the rpcs sent",
            )
            for (sent in transport.requests) {
                assertEquals(
                    "Bearer tok-bob",
                    sent.headers.entries.first { it.key.equals("Authorization", ignoreCase = true) }.value,
                    "the client's credential is the bearer",
                )
                val request = DynamicMessage.parseFrom(binding.request, sent.body)
                assertEquals("s1", request.getField(binding.request.findFieldByName("session_id")), "the session id")
            }
        }

        Harness.test("kotlin_live_authenticate_is_not_retried_and_session_reasons_are_known") {
            val transport = StubTransport()
            transport.answerConnectError(503, "unavailable", "restarting")
            transport.answerConnectError(503, "unavailable", "restarting")
            var failed = false
            try {
                transport.client(maxRetries = 3).modules.live.authenticate("s1")
            } catch (expected: dev.loams.LoamsException) {
                failed = true
            }
            assertTrue(failed, "authenticate to fail on a refusal")
            assertEquals(1, transport.requests.size, "an unkeyed mutation is sent once")
            for (wire in listOf("session_rotate", "token_expired", "token_revoked")) {
                assertTrue(ReasonRegistry.fromWire(wire) != null, "$wire to be a known reason")
            }
        }
    }
}
