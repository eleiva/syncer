const {PubSub} = require('@google-cloud/pubsub');
const {PrismaClient} = require('@prisma/client')

async function main() {

    const pubsub = new PubSub({
        projectId: 'api-project-464074989538',
        keyFilename: '/tmp/api-project-464074989538-302238e16c8e.json'
    });

    const prisma = new PrismaClient()

    pubsub.topic('synchronization');

    const subscription = pubsub.subscription('upg-main-sub');

    subscription.on('message', async message => {

        const data = JSON.parse(message.data.toString());

        if (data.action == "CREATE") {

            const customer = await prisma.customers.findFirst({
                where: {
                    centralization_id: Number(data.entity.customerID)
                }
            });

            const vendorProduct = await prisma.vendors_products.findFirst({
                where: {
                    centralization_id: Number(data.entity.supplierProductID)
                }
            });

            await prisma.vendors_products_customers_proprietaries.create({
                data: {
                    'customer_id' : customer.id,
                    'vendor_product_id' : vendorProduct.id
                }
            })
        }

        console.log('Received message:', message.data.toString());

        //message.ack();
        //console.log('-------------> Received acknowledge:');
    });

    subscription.on('error', error => {
        console.error('Received error:', error);
    });


}

main()