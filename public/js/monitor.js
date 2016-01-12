/**
 * Created by ahmed on 12/30/2015.
 */

var ws = new WebSocket('ws://localhost:5555/api/task/events/task-succeeded/');

ws.onmessage = function (event) {
    console.log(event);
};

ws.onopen = function (event) {
    console.group("OPEN");
    console.log(event);
    console.groupEnd();
};

ws.onclose = function (event) {
    console.group("CLOSE");
    console.error(event);
    console.groupEnd();
};

ws.onerror = function (event) {
    console.group("ERROR");
    console.error(event);
    console.groupEnd();

};
