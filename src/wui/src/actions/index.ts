import { defineAction } from "astro:actions"

export const server = {
    sayHello: defineAction({
        handler: async (input, ctx) => {
            return "hello"
        }
    })
}
