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
    }),
    getSchemaInfo: defineAction({
        input: z.object({
            schemaName: z.string()
        }),
        handler: async (input, ctx) => {
            const x = await (
                await fetch(`http://localhost:8000/${input.schemaName}`, {
                    method: "GET"
                })
            ).json()
            console.log(x)
            return x
        }
    }),
    createTable: defineAction({
        input: z.object({
            schemaName: z.string(),
            tableName: z.string(),
        }),
        handler: async (input, ctx) => {
            console.log(input)
            try {
                const x = await (
                    await fetch(`http://localhost:8000/${input.schemaName}/tables/${input.tableName}`, {
                        method: "POST",
                        body: JSON.stringify({
                            data: {
                                columns: [
                                    {
                                        name: "col1",
                                        dtype: "string",
                                        nullable: true
                                    }
                                ]
                            }
                        })
                    })
                ).json()
                console.log(x)
                return x
            }
            catch (exc) {
                console.log(exc)
            }
        }
    }),
    getTables: defineAction({
        input: z.object({
            schemaName: z.string()
        }),
        handler: async (input, ctx) => {
            const x = await (
                await fetch(`http://localhost:8000/${input.schemaName}/tables`, {
                    method: "GET"
                })
            ).json()
            console.log(x)
            return x
        }
    }),
    getTableRecords: defineAction({
        input: z.object({
            schemaName: z.string(),
            tableName: z.string(),
        }),
        handler: async (input, ctx) => {
            const x = await (
                await fetch(`http://localhost:8000/${input.schemaName}/${input.tableName}`, {
                    method: "GET"
                })
            ).json()
            console.log(x)
            return x
        }
    }),
    writeRecord: defineAction({
        input: z.object({
            schemaName: z.string(),
            tableName: z.string(),
        }),
        handler: async (input, ctx) => {
            const x = await (
                await fetch(`http://localhost:8000/${input.schemaName}/${input.tableName}`, {
                    method: "GET"
                })
            ).json()
            console.log(x)
            return x
        }
    }),
}
