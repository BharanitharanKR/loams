package dev.loams.test

import com.google.protobuf.DynamicMessage
import dev.loams.CallOptions
import dev.loams.Descriptors
import dev.loams.Facade
import dev.loams.IdempotencyLevel
import dev.loams.ReasonRegistry
import dev.loams.RetryClass
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
    }
}
