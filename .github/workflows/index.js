export default {
  async fetch(request) {
    return new Response("Hello World! 👋 from my Worker", {
      headers: { "content-type": "text/plain" },
    });
  },
};
