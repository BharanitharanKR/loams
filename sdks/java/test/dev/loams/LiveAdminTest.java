package dev.loams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.sun.net.httpserver.HttpServer;
import dev.loams.facade.CallBinding;
import dev.loams.facade.Facade;
import dev.loams.facade.ModuleBinding;
import dev.loams.facade.RetryClass;
import dev.loams.gen.loams.live.v1.CreateAppRequest;
import dev.loams.gen.loams.live.v1.DeleteAppRequest;
import dev.loams.gen.loams.live.v1.GetAppRequest;
import dev.loams.gen.loams.live.v1.ListAppsRequest;
import dev.loams.gen.loams.live.v1.UpdateAppRequest;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.Test;

/**
 * {@code loams.live.admin}'s app-directory calls (LV1 Task 13): the five calls reach
 * {@code LiveAdminService}, {@link CallOptions#withLiveApp} puts the app on the wire as the
 * {@code loams-live-app} header, and the {@code live_app_*} reasons are in the registry.
 */
public class LiveAdminTest {

    @Test
    public void theCatalogueNamesTheFiveCallsWithTheRightRetryClasses() {
        ModuleBinding module = Facade.module("live.admin").orElseThrow();
        assertEquals("loams.live.v1.LiveAdminService", module.service());
        assertTrue(module.unstable());
        Map<String, RetryClass> want =
                Map.of(
                        "CreateApp", RetryClass.MANUAL,
                        "DeleteApp", RetryClass.MANUAL,
                        "UpdateApp", RetryClass.MANUAL,
                        "GetApp", RetryClass.SAFE,
                        "ListApps", RetryClass.SAFE);
        assertEquals(want.size(), module.calls().size());
        for (CallBinding call : module.calls()) {
            assertEquals(call.name(), want.get(call.name()), call.retry());
            assertEquals("loams.live.v1.LiveAdminService/" + call.name(), call.rpc());
            assertTrue(call.name() + " takes no idempotency key", !call.takesIdempotencyKey());
        }
        assertEquals("apps", Facade.binding("live.admin", "ListApps").orElseThrow().pagination().itemsField());
        assertNull(Facade.binding("live.admin", "GetApp").orElseThrow().pagination());
        assertEquals("loams-live-app", Facade.LIVE_APP_HEADER);
    }

    @Test
    public void eachCallReachesTheAdminServiceAndWithLiveAppSetsTheHeader() throws IOException {
        List<String> paths = new CopyOnWriteArrayList<>();
        List<String> apps = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                exchange -> {
                    exchange.getRequestBody().readAllBytes();
                    paths.add(exchange.getRequestURI().getPath());
                    String app = exchange.getRequestHeaders().getFirst(Facade.LIVE_APP_HEADER);
                    apps.add(app == null ? "" : app);
                    String type = exchange.getRequestHeaders().getFirst("content-type");
                    byte[] body = "application/json".equals(type) ? "{}".getBytes() : new byte[0];
                    exchange.getResponseHeaders().set("content-type", type);
                    exchange.sendResponseHeaders(200, body.length == 0 ? -1 : body.length);
                    if (body.length > 0) {
                        exchange.getResponseBody().write(body);
                    }
                    exchange.close();
                });
        server.start();
        try {
            Client client =
                    Client.of(Options.builder().endpoint("http://127.0.0.1:" + server.getAddress().getPort()).build());
            Modules.LiveAdmin admin = client.liveAdmin();
            assertNotNull(admin);
            CallOptions blog = CallOptions.withLiveApp("blog");
            admin.createApp(CreateAppRequest.getDefaultInstance());
            admin.listApps(ListAppsRequest.getDefaultInstance());
            admin.getApp(GetAppRequest.getDefaultInstance(), blog);
            admin.updateApp(UpdateAppRequest.getDefaultInstance(), blog);
            admin.deleteApp(DeleteAppRequest.getDefaultInstance(), blog);
        } finally {
            server.stop(0);
        }
        List<String> names = new ArrayList<>(List.of("CreateApp", "ListApps", "GetApp", "UpdateApp", "DeleteApp"));
        for (int i = 0; i < names.size(); i++) {
            assertEquals("/loams.live.v1.LiveAdminService/" + names.get(i), paths.get(i));
        }
        assertEquals(List.of("", "", "blog", "blog", "blog"), apps);
    }

    @Test
    public void theLiveAppReasonsAreInTheRegistry() {
        for (String reason : List.of("live_app_disabled", "live_app_exists", "live_app_mismatch")) {
            assertNotNull(reason, dev.loams.facade.Reason.fromWire(reason));
        }
    }
}
