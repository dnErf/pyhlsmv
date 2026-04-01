import { defineAction } from "astro:actions"
import { z } from "astro/zod"

export const server = {
    sayHello: defineAction({
        handler: async (input, ctx) => {
            return "hello"
        }
    }),
    createSchema: defineAction({
        input: z.object({
            schemaName: z.string()
        }),
        handler: async (input, ctx) => {
            console.log(input)
            const x = await (
                await fetch(`http://localhost:8000/${input.schemaName}`, {
                    method: "POST"
                })
            ).json()
            console.log(x)
            return "hello"
        }
    })
}
